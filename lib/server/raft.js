/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const { ZMQ_LINGER } = require('zmq');
const { ZmqSocket } = require('../utils/zmqsocket');

const synchronize = require('../utils/synchronize');

const { allocBufUIntLE, readBufUIntLE, boolToBuffer, bufferToBool } = require('../utils/bufconv');

const { dispatchHandler } = require('../server/raft_router');
const { becomeLeader, updateFollower, onElectionTimeout } = require('../server/raft_leader');

const { assertConstantsDefined
      , createRangeRandomizer
      , parsePeers
      , isNonEmptyString
      , defineConst
      , majorityOf } = require('../utils/helpers');

const ReadyEmitter = require('../common/readyemitter');

const { createFramesProtocol } = require('../protocol');

const ZmqRpcSocket = require('../server/zmq_rpc_socket');

const nullBuf = Buffer.alloc(0);

const dispatchProtocol        = createFramesProtocol('Dispatch');
const requestVoteProtocol     = createFramesProtocol('RequestVote');
const appendEntriesProtocol   = createFramesProtocol('AppendEntries');
const installSnapshotProtocol = createFramesProtocol('InstallSnapshot');
const { encodeResponse: encodeRequestUpdateResponse } = createFramesProtocol('RequestUpdate');

const setReady$ = Symbol.for('setReady');

const debug = require('debug')('zmq-raft:service');

const { RPC_TIMEOUT
      , ELECTION_TIMEOUT_MIN
      , ELECTION_TIMEOUT_MAX

      , FSM_CLIENT
      , FSM_FOLLOWER
      , FSM_CANDIDATE
      , FSM_LEADER
      } = require('../common/constants');

assertConstantsDefined({
  RPC_TIMEOUT
, ELECTION_TIMEOUT_MIN
, ELECTION_TIMEOUT_MAX
}, 'number');

assertConstantsDefined({
  FSM_CLIENT
, FSM_FOLLOWER
, FSM_CANDIDATE
, FSM_LEADER
}, 'symbol');

/*

@property {string} myId - id of the peer instance
@property {Buffer} myIdBuf - id of the peer instance (as buffer)
@property {string} url - url of the peer instance if the instance is part of the peer cluster

@property {Map} peers - Map { 'id' => 'url' }
@property {Array} peersAry - [[id, url], ...]
@property {number} majority - a majority required to reach raft consensus

# RAFT
@property {Symbol} state
@property {number} currentTerm
@property {null|string} votedFor
@property {number} lastApplied
@property {number} commitIndex

@property {Map} nextIndex  - Map { follower id => nextIndex }
@property {Map} matchIndex - Map { follower id => matchIndex }
@property {Map} matchTerm  - Map { follower id => matchTerm }

*/
class ZmqRaft extends ReadyEmitter {
  /**
   * creates a new ZmqRaft peer instance
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
   * - secret {string}: a secret token for validating all messages (peers and clients)
   * - electionTimeoutMin=200 {number}: optional election min timeout
   * - electionTimeoutMax=300 {number}: optional election max timeout
   * - bindUrl|url {string}: optional url for binding a zmq-router socket
   *
   * @param {string} myId - a peer id identyfing this instance
   * @param {RaftPersistence} persistence - an instance used for storing raft persistence state
   * @param {FileLog} log - an instance used for storing log entries
   * @param {StateMachineBase} stateMachine - an instance used for updating state-machine
   * @param {Object} options
   * @return {FileLog}
  **/
  constructor(myId, persistence, log, stateMachine, options) {
    super();
    this._persistence = persistence;
    this._log = log;
    this._stateMachine = stateMachine;
    options || (options = {});

    /* forward raft state to state machine */
    this.on('state', (state, term) => stateMachine.emit('raft-state', state, term));

    if (!isNonEmptyString(myId)) {
      throw new Error("ZmqRaft: myId must be a non empty string");
    }

    defineConst(this, 'myId', myId);
    defineConst(this, 'myIdBuf', Buffer.from(myId));
    defineConst(this, '_secretBuf', Buffer.from(options.secret || ''));

    this.followLeaderId = null;
    this._electionTimeout = null;
    this._entryRequestCleaner = null;
    this._rndElectionTimeout = createRangeRandomizer(
        options.electionTimeoutMin || ELECTION_TIMEOUT_MIN,
        options.electionTimeoutMax || ELECTION_TIMEOUT_MAX);

    Promise.all([log.ready(), persistence.ready(), stateMachine.ready()]).then(() => {
      var {currentTerm, votedFor, peers} = persistence;

      /* parse peers */
      this.peers = peers = parsePeers(peers); // Map { id => url }
      this.peersAry = Array.from(this.peers); // [[id, url], ...]

      /* determine if this instance is a part of the cluster */
      if (peers.has(myId)) {
        this.url = peers.get(myId);
        peers.delete(myId);
      }

      /* a majority required to reach raft consensus */
      this.majority = majorityOf(peersAry.length);

      /* RAFT state */
      this.state = FSM_CLIENT;
      this.currentTerm = currentTerm;
      this.votedFor = votedFor;
      this.lastApplied = stateMachine.lastApplied;
      this.commitIndex = stateMachine.lastApplied;
      this.nextIndex  = new Map(); // Map { follower peerId => nextIndex }
      this.matchIndex = new Map(); // Map { follower peerId => matchIndex }
      this.matchTerm  = new Map(); // Map { follower peerId => matchTerm }

      /* how many voted for */
      this.votes = 0;
      /* tracking how many peers has voted (raft extension) */
      this.voted = new Set();

      /* followers' idle append entries heartbeat timeouts */
      this._heartbeats = new Map(); // Map { peerId => timeout }
      /* cache for buffers for sending entries and snapshots to followers */
      this._followerBuffers = new Map(); // Map { peerId => buffer {Buffer} }
      /* router request messages debounce cache */
      this._rpcLastRequestIds = new Map(); // Map { peerId => {ident: {buffer}, requestId: {null|number}, lastReplied: {undefined|Array} }

      /* temporary snapshot for install snapshot rpc */
      this._tmpSnapshot = null;
      this._tmpSnapshotBytesWritten = 0;

      /* client update requests waiting for a reply */
      this.updateRequests = new Map(); // Map { requestKey => [reply {Function}, index {null|number}] }
      /* client entries requests streams */
      this.entriesRequests = new Map(); // Map { requestKey => {expires: {number}, cancel: {Function}, request: {Function}} }

      this._rpcs = new Map(); // Map { peerId => rpc {ZmqRpcSocket} }

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

      var router = this._router = new ZmqSocket('router');
      router.setsockopt(ZMQ_LINGER, 2000);
      router.on('error', err => this.error(err));

      for(let [id, url] of peers) {
        let rpc = new ZmqRpcSocket(url, {timeout: RPC_TIMEOUT, sockopts: {ZMQ_RECONNECT_IVL: RPC_TIMEOUT}});
        this._rpcs.set(id, rpc);
        rpc.requestVote = requestVoteProtocol.createRequestFunctionFor(rpc);
        rpc.appendEntries = appendEntriesProtocol.createRequestFunctionFor(rpc);
        rpc.installSnapshot = installSnapshotProtocol.createRequestFunctionFor(rpc);
        this._rpcLastRequestIds.set(id, {ident: nullBuf, requestId: null});
      }

      return Promise.resolve().then(() => {
        if (this.url) {
          if (this.majority > 1) {
            this.state = FSM_FOLLOWER;
          }
          else {
            this.state = FSM_LEADER;
            return this._persistence.update({currentTerm: ++this.currentTerm})
            .then(() => becomeLeader.call(this));
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
          debug('term: %s votedFor: %j peers: %s', this.currentTerm, votedFor, peers.size);
          debug('commit: %s applied: %s', this.commitIndex, this.lastApplied);
          resolve();
        });
      }));
    })
    .then(() => this[setReady$]())
    .catch(err => this.error(err));
  }

  close() {
    var router = this._router
      , tmpSnapshot = this._tmpSnapshot;

    this.state = null;
    this._router = null;
    this._tmpSnapshot = null;
    this._cancelPendingRpcs();
    this._clearElectionTimeout();
    this._rpcs.clear();
    this._followerBuffers.clear();
    tmpSnapshot = tmpSnapshot ? tmpSnapshot.ready().then(s => s.close())
                              : Promise.resolve();
    return Promise.all([
      new Promise((resolve, reject) => {
        if (!router) return resolve();
        router.removeListener('frames', this._routerListener);
        router.on('error', reject);
        router.on('close', () => {
          debug('router closed');
          router.unmonitor();
          resolve();
        });
        router.monitor(10, 1);
        router.close();
      }),
      this._persistence.close(),
      this._log.close(),
      this._stateMachine.close(),
      tmpSnapshot
    ])
    .then(() => this.emit('close'))
    .catch(err => this.error(err));
  }

  get isLeader() {
    return this.state === FSM_LEADER;
  }

  _emitState() {
    try {
      this.emit('state', this.state, this.currentTerm);
    } catch(err) {
      this.error(err);
    }
  }

  // FOLLOWER -> CANDIDATE -> FOLLOWER
  //             CANDIDATE -> LEADER
  //                          LEADER -> FOLLOWER
  _updateState(state, currentTerm, votedFor) {
    var prevstate = this.state;
    this.state = state;
    if (prevstate !== FSM_FOLLOWER) this._cancelPendingRpcs();
    if (this.currentTerm !== currentTerm || this.votedFor !== votedFor) {
      this.votedFor = votedFor;
      this.currentTerm = currentTerm;
      this._emitState();
      return this._persistence.update({votedFor: votedFor, currentTerm: currentTerm});
    }
    else {
      if (prevstate !== state) this._emitState();
      return Promise.resolve();
    }
  }

  _clearElectionTimeout() {
    clearTimeout(this._electionTimeout);
    this._electionTimeout = null;
  }

  _refreshElectionTimeout(margin) {
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
    var updateRequests = this.updateRequests;
    if (updateRequests.size !== 0) {
      debug('clearing client update requests');
      var followLeaderId = this.followLeaderId;
      for(var [reply] of this.updateRequests.values()) {
        reply(encodeRequestUpdateResponse([false, followLeaderId]));
      }
      updateRequests.clear();
    }
    var entriesRequests = this.entriesRequests;
    if (entriesRequests.size !== 0) {
      debug('clearing client entries requests');
      for(var pending of entriesRequests.values()) pending.cancel();
    }
  }

  _updateFollowersNow() {
    if (this.majority === 1) {
      var log = this._log;
      if (this.commitIndex < log.lastIndex && log.lastTerm === this.currentTerm) {
        debug('commiting and applying state machine as a sole master: %s', log.lastIndex);
        this.commitIndex = log.lastIndex;
        this._applyToStateMachine();
      }
    }
    /* cancel peer's heartbeat timeouts and send append entries immediately if new entries await */
    else if (this._heartbeats.size !== 0) {
      for(var id of this._heartbeats.keys()) {
        updateFollower.call(this, id, this.nextIndex.get(id));
      }
    }
  }

  _applyToStateMachine() {
    return synchronize(this, () => {
      const log = this._log
          , commitIndex = this.commitIndex;
      var snapshot, nextIndex = this.lastApplied + 1;

      if (nextIndex > commitIndex) {
        debug('no need to update state machine with next: %s > commit: %s', nextIndex, commitIndex);
        return;
      }

      if (nextIndex < log.firstIndex) {
        nextIndex = log.firstIndex
        snapshot = log.snapshot;
        debug('applying snapshot to state machine with next: %s, commit: %s', nextIndex, commitIndex);
      }

      return this._log.getEntries(nextIndex, commitIndex)
      .then(entries => this._stateMachine.applyEntries(entries, nextIndex, this.currentTerm, snapshot))
      .then(lastApplied => {
        debug('updated state machine with last index: %s', lastApplied);
        this.lastApplied = lastApplied;
        var updateRequests = this.updateRequests;

        for(let [key, [reply, index]] of updateRequests) {
          if ('number' === typeof index && index <= lastApplied) {
            updateRequests.delete(key);
            reply(encodeRequestUpdateResponse([true, index]));
          }
        }
      });
    }).catch(err => this.error(err));
  }

}

module.exports = exports = ZmqRaft;
