/* 
 *  Copyright (c) 2016-2025 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const { ZMQ_LINGER } = require('zeromq');
const { ZmqSocket } = require('../utils/zmqsocket');

const synchronize = require('../utils/synchronize');

const { dispatchHandler } = require('../server/raft_router');
const { becomeLeader, updateFollower, onElectionTimeout } = require('../server/raft_leader');
const { synchronizeLogEntries } = require('../server/raft_peer_client');

const { assertConstantsDefined
      , createRangeRandomizer
      , parsePeers
      , isNonEmptyString
      , createOptionsFactory
      , validateIntegerOption
      , defineConst } = require('../utils/helpers');

const ReadyEmitter = require('../common/readyemitter');

const ClusterConfiguration = require('../common/cluster_configuration');

const { createFramesProtocol } = require('../protocol');

const ZmqRpcSocket = require('../server/zmq_rpc_socket');

const nullBuf = Buffer.alloc(0);

const dispatchProtocol        = createFramesProtocol('Dispatch');
const requestVoteProtocol     = createFramesProtocol('RequestVote');
const appendEntriesProtocol   = createFramesProtocol('AppendEntries');
const installSnapshotProtocol = createFramesProtocol('InstallSnapshot');

const setReady$ = Symbol.for('setReady');

const debug = require('debug')('zmq-raft:service');

const { RPC_TIMEOUT
      , ELECTION_TIMEOUT_MIN
      , ELECTION_TIMEOUT_MAX
      , APPEND_ENTRIES_HEARTBEAT_INTERVAL
      , APPEND_ENTRIES_RPC_TIMEOUT_MIN
      , APPEND_ENTRIES_RPC_TIMEOUT_MAX
      , MAX_LOG_ENTRY_DATA_SIZE
      , PEER_MSG_DATA_SIZE
      , REQUEST_ID_TTL_ACCEPT_MARGIN
      , REQUEST_ENTRIES_HIGH_WATERMARK
      , REQUEST_ENTRIES_TTL
      , REQUEST_ENTRIES_PIPELINES
      , REQUEST_ENTRIES_ENTRY_SIZE_LIMIT_PIPELINE
      , REQUEST_ENTRIES_SNAPSHOT_PIPELINES

      , FSM_CLIENT
      , FSM_FOLLOWER
      , FSM_CANDIDATE
      , FSM_LEADER
      } = require('../common/constants');

const { makeCheckRequestSanity } = require('../common/log_entry');

assertConstantsDefined({
  RPC_TIMEOUT
, ELECTION_TIMEOUT_MIN
, ELECTION_TIMEOUT_MAX
, APPEND_ENTRIES_HEARTBEAT_INTERVAL
, APPEND_ENTRIES_RPC_TIMEOUT_MIN
, APPEND_ENTRIES_RPC_TIMEOUT_MAX
, MAX_LOG_ENTRY_DATA_SIZE
, PEER_MSG_DATA_SIZE
, REQUEST_ID_TTL_ACCEPT_MARGIN
, REQUEST_ENTRIES_HIGH_WATERMARK
, REQUEST_ENTRIES_TTL
, REQUEST_ENTRIES_PIPELINES
, REQUEST_ENTRIES_ENTRY_SIZE_LIMIT_PIPELINE
, REQUEST_ENTRIES_SNAPSHOT_PIPELINES
}, 'number');

assertConstantsDefined({
  FSM_CLIENT
, FSM_FOLLOWER
, FSM_CANDIDATE
, FSM_LEADER
}, 'symbol');

const createRaftOptions = createOptionsFactory({
  electionTimeoutMin: ELECTION_TIMEOUT_MIN
, electionTimeoutMax: ELECTION_TIMEOUT_MAX
, maxLogEntryDataSize: MAX_LOG_ENTRY_DATA_SIZE
, peerMsgDataSize: PEER_MSG_DATA_SIZE
, rpcTimeout: RPC_TIMEOUT
, appendEntriesHeartbeatInterval: APPEND_ENTRIES_HEARTBEAT_INTERVAL
, appendEntriesRpcTimeoutMin: APPEND_ENTRIES_RPC_TIMEOUT_MIN
, appendEntriesRpcTimeoutMax: APPEND_ENTRIES_RPC_TIMEOUT_MAX
, requestIdTtlAcceptMargin: REQUEST_ID_TTL_ACCEPT_MARGIN
, requestEntriesHighWatermak: REQUEST_ENTRIES_HIGH_WATERMARK
, requestEntriesTtl: REQUEST_ENTRIES_TTL
, requestEntriesPipelines: REQUEST_ENTRIES_PIPELINES
, requestEntriesEntrySizeLimitPipeline: REQUEST_ENTRIES_ENTRY_SIZE_LIMIT_PIPELINE
, requestEntriesSnapshotPipelines: REQUEST_ENTRIES_SNAPSHOT_PIPELINES
, preventSpiralElections: true
});

/*

@property {string} peerId - id of the peer instance
@property {Buffer} peerIdBuf - id of the peer instance (as buffer)
@property {string|undefined} url - url of the peer instance if the instance is part of the peer cluster

@property {ClusterConfiguration} cluster
@property {Map} peers - Map { 'id' => 'url' } of other peers (except this peer)

# RAFT
@property {Symbol} state
@property {number} currentTerm
@property {null|string} votedFor
@property {number} lastApplied
@property {number} commitIndex

@property {Map} nextIndex  - Map { followerId => nextIndex }
@property {Map} matchIndex - Map { followerId => matchIndex }
@property {Map} matchTerm  - Map { followerId => matchTerm }
@property {Set} slowHBeat  - Set { followerId }

*/
class ZmqRaft extends ReadyEmitter {
  /**
   * Creates a new ZmqRaft peer instance.
   *
   * A `persistence` instance should provide persistent state of RAFT properties:
   *
   * - currentTerm {number}
   * - votedFor {null|string}
   * - peers {Array}
   *
   * A persistent state is updated via PersistenceBase API's update() method.
   *
   * If the stateMachine is persistent it may store it's lastApplied property
   * it is being used for the initial state of the lastApplied and commitIndex Raft state variables.
   *
   * options:
   *
   * - secret {string}: A secret token for validating all messages (peers and clients).
   * - bindUrl|url {string}: An optional url for binding a zmq-router socket.
   * - electionTimeoutMin=200 {number}: An optional election min timeout.
   * - electionTimeoutMax=300 {number}: An optional election max timeout.
   * - maxLogEntryDataSize=10485760 {number}: A maximum log entry data bytes (hard limit)
   *                                          enforced by RequestUpdate server handler.
   * - peerMsgDataSize=65536 {number}: A hint how many bytes are send to followers via each
   *                                   AppendEntries or InstallSnapshot message
   *                                   increasing it will improve update throughput
   *                                   but setting it too high affects server responsiveness
   *                                   and memory consumption.
   * - rpcTimeout=50 {number}: A request vote RPC timeout.
   * - appendEntriesHeartbeatInterval=70 {number}: An Append Entries RPC heartbeat interval.
   * - appendEntriesRpcTimeoutMin=70 {number}: An Append Entires RPC timeout without entries.
   * - appendEntriesRpcTimeoutMax=140 {number}: An Append Entires RPC timeout with entries
   *                                           and install snapshot RPC timeout.
   * - requestIdTtlAcceptMargin=900000 {number}: A timestamp margin of request ID in milliseconds;
   *                                   when accepting a new log updates, the timestamp part of
   *                                   an updating request ID must be less than now + margin and
   *                                   must be greater than now + margin - log.requestIdTtl.
   * - requestEntriesHighWatermak=8000 {number}: A Request Entries RPC high watermark.
   * - requestEntriesTtl=5000 {number}: A Request Entries RPC pipeline TTL in milliseconds.
   * - requestEntriesPipelines=5 {number}: A number of Request Entries RPC pipelined response messages.
   * - requestEntriesEntrySizeLimitPipeline=65536 {number}: A limit in bytes of a pipelined response message size.
   *                                              Log entries of this size or larger will limit a pipeline temporarely.
   * - requestEntriesSnapshotPipelines=2 {number}: A number of Request Entries RPC pipelined response messages
   *                                               containing snapshot chunks.
   * - preventSpiralElections=true {boolean}: Prevents spiraling of new elections on partitioned peers.
   *
   * NOTE: If the `log.requestIdTtl` is `null` time based validation of request ID will NOT be performed.
   *
   * @param {string} peerId - a peer id identifying this instance
   * @param {RaftPersistence} persistence - an instance used for storing raft persistence state
   * @param {FileLog} log - an instance used for storing log entries
   * @param {StateMachineBase} stateMachine - an instance used for updating state-machine
   * @param {Object} options
   * @return {ZmqRaft}
  **/
  constructor(peerId, persistence, log, stateMachine, options) {
    super();
    this._persistence = persistence;
    this._log = log;
    this._stateMachine = stateMachine;
    options = createRaftOptions(options);

    /* forward raft state to the state machine */
    this.on('state', (state, term) => stateMachine.emit('raft-state', state, term));

    if (!isNonEmptyString(peerId)) {
      throw new Error("ZmqRaft: peerId must be a non empty string");
    }

    defineConst(this, 'peerId', peerId);
    defineConst(this, 'peerIdBuf', Buffer.from(peerId));
    defineConst(this, '_secretBuf', Buffer.from(options.secret || ''));

    this.followLeaderId = null;
    this._electionTimeout = null;
    this._entryRequestCleaner = null;
    var electionTimeoutMin = validateIntegerOption(options, 'electionTimeoutMin', 100);
    var electionTimeoutMax = validateIntegerOption(options, 'electionTimeoutMax', electionTimeoutMin);
    this._rndElectionTimeout = createRangeRandomizer(electionTimeoutMin, electionTimeoutMax);

    /* the following options can be run-time adjusted */
    this.maxLogEntryDataSize = validateIntegerOption(options, 'maxLogEntryDataSize', 0);
    this.peerMsgDataSize     = validateIntegerOption(options, 'peerMsgDataSize', 64);

    var requestIdTtlAcceptMargin = validateIntegerOption(options, 'requestIdTtlAcceptMargin', 1000);
    defineConst(this, 'checkRequestSanity',
        makeCheckRequestSanity(log.requestIdTtl, requestIdTtlAcceptMargin, requestIdTtlAcceptMargin));

    defineConst(this, 'rpcTimeout',
      validateIntegerOption(options, 'rpcTimeout', 10, electionTimeoutMin));
    defineConst(this, 'appendEntriesHeartbeatInterval',
      validateIntegerOption(options, 'appendEntriesHeartbeatInterval', this.rpcTimeout, electionTimeoutMin));
    defineConst(this, 'appendEntriesRpcTimeoutMin',
      validateIntegerOption(options, 'appendEntriesRpcTimeoutMin', this.rpcTimeout, electionTimeoutMin));
    defineConst(this, 'appendEntriesRpcTimeoutMax',
      validateIntegerOption(options, 'appendEntriesRpcTimeoutMax', this.appendEntriesRpcTimeoutMin, electionTimeoutMin));

    defineConst(this, 'requestEntriesHighWatermak',
      validateIntegerOption(options, 'requestEntriesHighWatermak', 0));
    defineConst(this, 'requestEntriesTtl',
      validateIntegerOption(options, 'requestEntriesTtl', 100));
    defineConst(this, 'requestEntriesPipelines',
      validateIntegerOption(options, 'requestEntriesPipelines', 1, 50));
    defineConst(this, 'requestEntriesEntrySizeLimitPipeline',
      validateIntegerOption(options, 'requestEntriesEntrySizeLimitPipeline', 1));
    defineConst(this, 'requestEntriesSnapshotPipelines',
      validateIntegerOption(options, 'requestEntriesSnapshotPipelines', 1, 50));
    defineConst(this, 'preventSpiralElections', !!options.preventSpiralElections);

    Promise.all([log.ready(), persistence.ready(), stateMachine.ready()]).then(() => {
      var {currentTerm, votedFor, peers, peersUpdateRequest, peersIndex} = persistence;

      /* parse cluster config */
      this.cluster = new ClusterConfiguration(peerId, peers);
      /* it's safe to share peers with cluster config
         they will be updated with cluster peer membership changes */
      peers = this.peers = this.cluster.peers  // Map { id => url }
      this.peersUpdateRequest = peersUpdateRequest;
      this.peersIndex = peersIndex;
      this.blacklistedPeers = new Set();
      /* RAFT state */
      this.state = FSM_CLIENT;
      this.currentTerm = currentTerm;
      this.votedFor = votedFor;
      this.lastApplied = stateMachine.lastApplied;
      this.commitIndex = stateMachine.lastApplied;
      this.nextIndex  = new Map(); // Map { peerId => nextIndex }
      this.matchIndex = new Map(); // Map { peerId => matchIndex }
      this.matchTerm  = new Map(); // Map { peerId => matchTerm }
      this.slowHBeat = new Set(); // Set { peerId }

      /* followers' idle append entries heartbeat timeouts */
      this._heartbeats = new Map(); // Map { peerId => timeout }
      /* cache for buffers for sending entries and snapshots to followers */
      this._followerBuffers = new Map(); // Map { peerId => buffer {Buffer} }
      /* router request messages debounce cache */
      this._rpcLastRequestIds = new Map(); // Map { peerId => {ident: {buffer}, requestId: {null|number}, lastReplied: {undefined|Array} }

      /* temporary snapshot for install snapshot rpc */
      this._tmpSnapshot = null;
      this._tmpSnapshotBytesWritten = 0;

      /* client config update waiting for a reply */
      this.configUpdateReply = null;
      /* client update requests waiting for a reply */
      this.updateRequests = new Map(); // Map { requestKey => [reply {Function}, index {null|number}] }
      /* client entries requests streams */
      this.entriesRequests = new Map(); // Map { requestKey => {expires: {number}, cancel: {Function}, request: {Function}} }

      /* communication channels with peers for RequestVote, AppendEntries and InstallSnapshot */
      this._rpcs = new Map(); // Map { peerId => rpc {ZmqRpcSocket} }

      if (this.cluster.isTransitional && peersUpdateRequest == null) {
        throw new Error("ZmqRaft: sanity error - transitional config without request key");
      }
      if (peersUpdateRequest != null && peersIndex == null) {
        throw new Error("ZmqRaft: sanity error - config in the log not confirmed");
      }
      if (peersIndex != null && peersIndex > log.lastIndex) {
        throw new Error("ZmqRaft: sanity error - config not appended to the log");
      }
      if (stateMachine.lastApplied > log.lastIndex) {
        throw new Error("ZmqRaft: sanity error - state machine lastApplied is above the log last index entry");
      }
      if (log.firstIndex > stateMachine.lastApplied + 1) {
        throw new Error("ZmqRaft: sanity error - state machine is not up to the first log entry");
      }
      if (log.lastTerm > currentTerm) {
        throw new Error(`ZmqRaft: sanity error - raft state is not up to the last log entry's term: ${currentTerm} < ${log.lastTerm}`);
      }
      if (log.snapshot.logIndex === 0 && log.snapshot.logTerm !== 0) {
        throw new Error(`ZmqRaft: sanity error - snapshot with index 0 must have term 0, found: ${log.snapshot.logTerm}`);
      }
      if (log.snapshot.logIndex !== log.firstIndex - 1) {
        throw new Error(`ZmqRaft: sanity error - snapshot must precede log first index, found: ${log.snapshot.logIndex}`);
      }

      const router = this._router = new ZmqSocket('router');
      router.setsockopt(ZMQ_LINGER, 2000);
      router.on('error', err => this.error(err));

      for(let [id, url] of peers) this._addPeerRpc(id, url);

      return (this.cluster.isMember(peerId) ? Promise.resolve()
                                            : synchronizeLogEntries.call(this)
      ).then(() => {
        if (this.cluster.isMember(peerId)) {
          if (this.cluster.isSoleMaster) {
            this.state = FSM_LEADER;
            return this._persistence.update({currentTerm: ++this.currentTerm})
            .then(() => becomeLeader.call(this));
          }
          else {
            this.state = FSM_FOLLOWER;
          }
        }
      })
      .then(() => new Promise((resolve, reject) => {
        var bindUrl = this.routerBindUrl = options.bindUrl || options.url || this.url;
        this._routerListener = dispatchProtocol.createRouterMessageListener(router, dispatchHandler, this);
        router.bind(bindUrl, err => {
          if (err) return reject(err);
          if (this.state === FSM_FOLLOWER) {
            this._refreshElectionTimeout(ELECTION_TIMEOUT_MAX);
          }
          this._emitState();
          debug('ready at: %s', bindUrl);
          debug('term: %s votedFor: %j', this.currentTerm, votedFor);
          debug('peers: %s peersUpdateRequest: %s peersIndex: %s', peers.size, peersUpdateRequest, peersIndex);
          debug('commit: %s applied: %s', this.commitIndex, this.lastApplied);
          resolve();
        });
      }));
    })
    .then(() => this[setReady$]())
    .catch(err => this.error(err));
  }
  /**
   * Shuts down this ZmqRaft peer instance.
   *
   * @return {Promise}
  **/
  close() {
    var router = this._router
      , tmpSnapshot = this._tmpSnapshot;

    this.state = null;
    this._router = null;
    this._tmpSnapshot = null;
    this._cancelPendingRpcs();
    this._clearElectionTimeout();
    for(let rpc of this._rpcs.values()) rpc.destroy();
    this._rpcs.clear();
    this._rpcLastRequestIds.clear();
    this._followerBuffers.clear();
    tmpSnapshot = tmpSnapshot ? tmpSnapshot.ready().then(s => s.close())
                              : Promise.resolve();
    if (router) {
      debug('closing');
      router.removeListener('frames', this._routerListener);
      router.close();
    }

    return Promise.all([
      this._persistence.close(),
      this._log.close(),
      this._stateMachine.close(),
      tmpSnapshot
    ])
    .then(() => this.emit('close'))
    .catch(err => this.error(err));
  }
  /**
   * @property {string|undefined} - this peer's router url if the peer is a part of the cluster
  **/
  get url() {
    /* determine if this instance is a part of the cluster config */
    return this.cluster.getUrl(this.peerId);
  }
  /**
   * @property {Array} peersAry - a copy of all current cluster peers' tuples: `[id, url]`
  **/
  get peersAry() {
    return this.cluster.configAry.map(pair => pair.slice(0));
  }
  /**
   * @property {boolean}
  **/
  get isLeader() {
    return this.state === FSM_LEADER;
  }
  /**
   * @property {number}
  **/
  get pruneIndex() {
    const log = this._log;
    var firstFreshIndex = log.getFirstFreshIndex();
    return firstFreshIndex === undefined ? log.lastIndex
                                         : firstFreshIndex - 1;
  }

  _addPeerRpc(peerId, url) {
    var rpc = createPeerRpc(url, this.rpcTimeout);
    this._rpcs.set(peerId, rpc);
    this._rpcLastRequestIds.set(peerId, {ident: nullBuf, requestId: null});
  }

  _emitState() {
    try {
      this.emit('state', this.state, this.currentTerm);
    } catch(err) {
      this.error(err);
    }
  }

  _emitConfig(cfg) {
    try {
      this.emit('config', cfg);
    } catch(err) {
      this.error(err);
    }
  }

  _peerIdInCluster(peerId, label) {
    if (!this.peers.has(peerId)) {
      var blacklistedPeers = this.blacklistedPeers;
      if (!blacklistedPeers.has(peerId)) {
        blacklistedPeers.add(peerId);
        debug('%s: no such peer: %s', label, peerId);
      }
      return false;
    }
    else return true;
  }


  // CLIENT -> FOLLOWER
  // CLIENT                          -> LEADER
  //           FOLLOWER -> CANDIDATE           -> FOLLOWER
  //                       CANDIDATE -> LEADER
  //                                    LEADER -> FOLLOWER
  _updateState(state, currentTerm, votedFor, peers, peersUpdateRequest, peersIndex) {
    var prevState = this.state
      , prevTerm = this.currentTerm;

    if (prevState === FSM_CANDIDATE || prevState === FSM_LEADER) {
      this._cancelPendingRpcs();
    }

    if (peers !== undefined) this._updateConfig(peers, peersUpdateRequest, peersIndex);

    if (state === FSM_FOLLOWER && !this.cluster.isMember(this.peerId)) {
      state = FSM_CLIENT;
    }

    if (state === FSM_CLIENT) this._clearElectionTimeout();

    this.state = state;

    if (prevTerm !== currentTerm || this.votedFor !== votedFor) {
      this.votedFor = votedFor;
      this.currentTerm = currentTerm;
      if (prevState !== state || prevTerm !== currentTerm) this._emitState();
      return this._persistence.update({votedFor, currentTerm, peers, peersUpdateRequest, peersIndex});
    }
    else {
      if (prevState !== state) this._emitState();
      if (peers !== undefined) {
        return this._persistence.update({peers, peersUpdateRequest, peersIndex});
      }
      else return Promise.resolve();
    }
  }

  _updateConfig(peers, peersUpdateRequest, peersIndex) {
    var cluster = this.cluster
      , heartbeats = this._heartbeats
      , state = this.state;

    cluster.replace(peers);
    this.peersUpdateRequest = peersUpdateRequest;
    this.peersIndex = peersIndex;
    this.blacklistedPeers.clear();
    debug('cluster membership configuration updated: %j', peers);
    /* update other peers heartbeats, rpcs, debouncers and caches */
    cluster.updateOtherPeersMap(heartbeats,
      undefined, /* only delete gone peers' heartbeats */
      (timeout, peerId) => clearTimeout(timeout));
    cluster.updateOtherPeersMap(this._rpcs,
      state !== FSM_LEADER ? (url) => createPeerRpc(url, this.rpcTimeout)
      /* leader-only: mark heartbeats for new followers to be updated */
                           : (url, peerId) => {
                              heartbeats.set(peerId, null);
                              return createPeerRpc(url, this.rpcTimeout);
                            },
      (rpc, peerId) => rpc.destroy());
    cluster.updateOtherPeersMap(this._rpcLastRequestIds,
      (url, peerId) => ({ident: nullBuf, requestId: null}));
    cluster.updateOtherPeersMap(this._followerBuffers);
    if (state === FSM_LEADER) {
      /* add/remove states for new/old peers */
      cluster.resetOtherPeersMap(this.nextIndex, this._log.lastIndex + 1);
      cluster.resetOtherPeersMap(this.matchIndex, 0);
      cluster.resetOtherPeersMap(this.matchTerm, 0);
      this.slowHBeat = new Set(this.peers.keys());
    }
    this._emitConfig(peers);
  }

  _clearElectionTimeout() {
    var timeout = this._electionTimeout;
    this._electionTimeout = null;
    if (timeout != null) clearTimeout(timeout);
  }

  _refreshElectionTimeout(margin) {
    var state = this.state;
    if (state === FSM_CLIENT || state === FSM_LEADER) return this._clearElectionTimeout();
    margin || (margin = 0);
    clearTimeout(this._electionTimeout);
    this._electionTimeout = setTimeout(() => onElectionTimeout.call(this), this._rndElectionTimeout() + margin);
  }

  _cancelPendingRpcs() {
    for(let rpc of this._rpcs.values()) rpc.reset();
    for(let timeout of this._heartbeats.values()) clearTimeout(timeout);
    this._heartbeats.clear();
    this._clearClientRequests();
  }

  _clearClientRequests() {
    var followLeaderId = this.followLeaderId
      , updateRequests = this.updateRequests;
    if (updateRequests.size !== 0) {
      debug('clearing client update requests');
      for(var [sendReply] of this.updateRequests.values()) {
        sendReply(false, followLeaderId);
      }
      updateRequests.clear();
    }
    var entriesRequests = this.entriesRequests;
    if (entriesRequests.size !== 0) {
      debug('clearing client entries requests');
      for(var pending of entriesRequests.values()) pending.cancel();
    }
    var configUpdateReply = this.configUpdateReply;
    this.configUpdateReply = null;
    if (configUpdateReply) configUpdateReply(0, followLeaderId);
  }

  _updateFollowersNow() {
    if (this.cluster.isSoleMaster) {
      const log = this._log;
      if (this.commitIndex < log.lastIndex && log.lastTerm === this.currentTerm) {
        debug('committing and applying state machine as a sole master: %s', log.lastIndex);
        this.commitIndex = log.lastIndex;
        this._applyToStateMachine();
      }
    }
    /* cancel peer's heartbeat timeouts and send append entries immediately if new entries await */
    else if (this._heartbeats.size !== 0) {
      for(let id of this._heartbeats.keys()) {
        /* heartbeats are setup if a follower is up to date with log entries
           so assume previous success here and allow sending many entries at once */
        updateFollower.call(this, id, this.nextIndex.get(id), true);
      }
    }
  }

  _applyToStateMachine() {
    return synchronize(this, () => {
      const log = this._log
          , commitIndex = this.commitIndex
          , nextIndex = this.lastApplied + 1;

      if (nextIndex > commitIndex) {
        debug('no need to update state machine with next: %s > commit: %s', nextIndex, commitIndex);
        return;
      }

      if (nextIndex < log.firstIndex) {
        debug('applying snapshot to state machine with next: %s, commit: %s', log.firstIndex, commitIndex);
      }

      return log.feedStateMachine(this._stateMachine, commitIndex, this.currentTerm)
      .then(lastApplied => {
        debug('updated state machine with last index: %s', lastApplied);
        this.lastApplied = lastApplied;
        var updateRequests = this.updateRequests;

        for(let [key, [sendReply, index]] of updateRequests) {
          if ('number' === typeof index && index <= lastApplied) {
            updateRequests.delete(key);
            sendReply(true, index);
          }
        }
      });
    }).catch(err => this.error(err));
  }

}

function createPeerRpc(url, rpcTimeout=RPC_TIMEOUT) {
  var rpc = new ZmqRpcSocket(url, {timeout: rpcTimeout, sockopts: {ZMQ_RECONNECT_IVL: rpcTimeout}});
  rpc.requestVote = requestVoteProtocol.createRequestFunctionFor(rpc);
  rpc.appendEntries = appendEntriesProtocol.createRequestFunctionFor(rpc);
  rpc.installSnapshot = installSnapshotProtocol.createRequestFunctionFor(rpc);
  return rpc;
}

module.exports = exports = ZmqRaft;
