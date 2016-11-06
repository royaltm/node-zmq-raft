/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const { ZMQ_LINGER } = require('zmq');
const { ZmqSocket } = require('../utils/zmqsocket');

const synchronize = require('../utils/synchronize');

const { allocBufUIntLE, readBufUIntLE, boolToBuffer, bufferToBool } = require('../utils/bufconv');

const { dispatchHandler } = require('../server/raft_router');
const { becomeLeader, updateFollower, onElectionTimeout } = require('../server/raft_leader');

const { assertConstantsDefined, createRangeRandomizer, parsePeers } = require('../utils/helpers');

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

const debug = require('debug')('zmq-raft');

/*

raft = new Raft(myId, persistence, log, stateMachine, options)

*/

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

class ZmqRaft extends ReadyEmitter {
  constructor(myId, persistence, log, stateMachine, options) {
    super();
    this._persistence = persistence;
    this._log = log;
    this._stateMachine = stateMachine;

    /* forward raft state to state machine */
    this.on('state', (state, term) => stateMachine.emit('raft-state', state, term));

    this.myId = myId;
    this.myIdBuf = Buffer.from(myId);

    this._secretBuf = Buffer.from(options.secret || '');

    this.followLeaderId = null;
    this._electionTimeout = null;
    this._entryRequestCleaner = null;
    this._rndElectionTimeout = createRangeRandomizer(
        options.electionTimeoutMin || ELECTION_TIMEOUT_MIN,
        options.electionTimeoutMax || ELECTION_TIMEOUT_MAX);

    Promise.all([log.ready(), persistence.ready(), stateMachine.ready()]).then(() => {
      var {currentTerm, votedFor, peers} = persistence;
      this.currentTerm = currentTerm;
      this.votedFor = votedFor;

      /* parse peers */
      this.peers = peers = parsePeers(peers); // [{url: 'tcp://x:y', id: 'someid'}]
      this.peersAry = Array.from(this.peers);

      if (peers.has(myId)) {
        this.url = peers.get(myId);
        peers.delete(myId);
      }

      this.majority = ((peers.size + 1) >>> 1) + 1;

      this.state = FSM_CLIENT;

      this.lastApplied = stateMachine.lastApplied;
      this.commitIndex = stateMachine.lastApplied; // IS IT SAFE TO ASSUME IT?

      this.nextIndex  = new Map();
      this.matchIndex = new Map();
      this.matchTerm  = new Map();

      this.votes = 0;
      this.voted = new Set();

      this._heartbeats = new Map();
      this._followerBuffers = new Map();
      this._rpcLastRequestIds = new Map();
      this._tmpSnapshot = null;
      this._tmpSnapshotBytesWritten = 0;

      this.updateRequests = new Map();
      this.entriesRequests = new Map();

      this._rpcs = new Map();

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
      })
    }).catch(err => this.error(err));
  }

}

module.exports = exports = ZmqRaft;

