/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const now = Date.now
    , min = Math.min;

const assert = require('assert');
const zmq = require('zmq');
const debug = require('debug')('zmq-raft-router');

const { decodeRequestId, requestIdIsValid } = require('../server/zmq_rpc_socket');

const { assertConstantsDefined } = require('../utils/helpers');

const { FSM_CLIENT
      , FSM_FOLLOWER
      , FSM_CANDIDATE
      , FSM_LEADER

      , APPEND_ENTRY
      , REQUEST_VOTE
      , INSTALL_SNAPSHOT
      , REQUEST_UPDATE
      , REQUEST_ENTRIES
      , REQUEST_CONFIG
      , REQUEST_LOG_INFO
      } = require('../common/constants');

assertConstantsDefined({
  APPEND_ENTRY
, REQUEST_VOTE
, INSTALL_SNAPSHOT
, REQUEST_UPDATE
, REQUEST_ENTRIES
, REQUEST_CONFIG
, REQUEST_LOG_INFO
}, 'string', true);

assertConstantsDefined({
  FSM_CLIENT
, FSM_FOLLOWER
, FSM_CANDIDATE
, FSM_LEADER
}, 'symbol');

const APPEND_ENTRY_MATCH     = (APPEND_ENTRY).charCodeAt(0)
    , REQUEST_VOTE_MATCH     = (REQUEST_VOTE).charCodeAt(0)
    , INSTALL_SNAPSHOT_MATCH = (INSTALL_SNAPSHOT).charCodeAt(0)
    , REQUEST_UPDATE_MATCH   = (REQUEST_UPDATE).charCodeAt(0)
    , REQUEST_ENTRIES_MATCH  = (REQUEST_ENTRIES).charCodeAt(0)
    , REQUEST_CONFIG_MATCH   = (REQUEST_CONFIG).charCodeAt(0)
    , REQUEST_LOG_INFO_MATCH = (REQUEST_LOG_INFO).charCodeAt(0)

const { requestUpdateHandler
      , requestEntriesHandler
      , requestConfigHandler
      , requestLogInfoHandler } = require('../server/raft_server');

const { createFramesProtocol } = require('../protocol');

const {
  decodeRequest:  decodeAppendEntriesRequest,
  encodeResponse: encodeAppendEntriesResponse }   = createFramesProtocol('AppendEntries');
const {
  decodeRequest:  decodeRequestVoteRequest,
  encodeResponse: encodeRequestVoteResponse }     = createFramesProtocol('RequestVote');
const {
  decodeRequest:  decodeInstallSnapshotRequest,
  encodeResponse: encodeInstallSnapshotResponse } = createFramesProtocol('InstallSnapshot');
const {
  decodeRequest:  decodeRequestUpdateRequest }    = createFramesProtocol('RequestUpdate');
const {
  decodeRequest:  decodeRequestEntriesRequest }   = createFramesProtocol('RequestEntries');
const {
  decodeRequest:  decodeRequestConfigRequest }    = createFramesProtocol('RequestConfig');
const {
  decodeRequest:  decodeRequestLogInfoRequest }   = createFramesProtocol('RequestLogInfo');

const handlers = {
  [APPEND_ENTRY_MATCH]:     { decodeRequest: decodeAppendEntriesRequest,   handler: appendEntryHandler }
, [REQUEST_VOTE_MATCH]:     { decodeRequest: decodeRequestVoteRequest,     handler: requestVoteHandler }
, [INSTALL_SNAPSHOT_MATCH]: { decodeRequest: decodeInstallSnapshotRequest, handler: installSnapshotHandler }
, [REQUEST_UPDATE_MATCH]:   { decodeRequest: decodeRequestUpdateRequest,   handler: requestUpdateHandler }
, [REQUEST_ENTRIES_MATCH]:  { decodeRequest: decodeRequestEntriesRequest,  handler: requestEntriesHandler }
, [REQUEST_CONFIG_MATCH]:   { decodeRequest: decodeRequestConfigRequest,   handler: requestConfigHandler }
, [REQUEST_LOG_INFO_MATCH]: { decodeRequest: decodeRequestLogInfoRequest,  handler: requestLogInfoHandler }
};

exports.dispatchHandler = function(reply, args) {
  const [msgType, secret] = args
      , typeLength = msgType.length;

  if (!this._secretBuf.equals(secret)) {
    debug('router: message auth fail');
  }
  else {
    if (typeLength === 1) {
      let type = msgType[0];
      switch(type) {
        /* peers */
        case APPEND_ENTRY_MATCH:
        case REQUEST_VOTE_MATCH:
        case INSTALL_SNAPSHOT_MATCH:
          if (!requestIdIsValid(reply.requestId)) {
            debug('invalid requestId from peer');
            return;
          }
        /* clients */
        case REQUEST_UPDATE_MATCH:
        case REQUEST_ENTRIES_MATCH:
        case REQUEST_CONFIG_MATCH:
        case REQUEST_LOG_INFO_MATCH:
          var {decodeRequest, handler} = handlers[type];
          args = decodeRequest(args).slice(2);
          handler.call(this, reply, ...args);
          return;
      }
    }
    if (typeLength >= 1) {
      /* forward request to state machine */
      this._stateMachine.emit('client-request', reply, msgType, args.slice(2));
    }
    else {
      /* drop it */
      debug('dropping unknown message: "%s"', msgType);
    }
  }
};


function bounceDuplicateRequestCache(rpcLastRequestIds, peerId, reply) {
  const ident = reply.ident
      , requestId = decodeRequestId(reply.requestId);

  var cache = rpcLastRequestIds.get(peerId);

  if (cache.requestId === requestId && cache.ident.equals(ident)) {
    /* bounce */
    if (cache.lastReplied !== undefined) reply(cache.lastReplied);
    return;
  }
  else {
    cache = {ident, requestId};
    rpcLastRequestIds.set(peerId, cache);
    return cache;
  }
}

/* peer handlers */

function requestVoteHandler(reply, candidateId, candidateTerm, candidateLastIndex, candidateLastTerm) {
  if (!this.peers.has(candidateId)) {
    debug('request vote rpc: no such peer: %s', candidateId);
    return;
  }

  const log = this._log;
  var votedFor = this.votedFor
    , currentTerm = this.currentTerm
    , voteGranted;

  const sendReply = () => reply(encodeRequestVoteResponse([currentTerm, voteGranted]));

  // §5.1
  if (candidateTerm < currentTerm) {
    debug('candidate term too low: %s < %s', candidateTerm, currentTerm);
    voteGranted = false;
    sendReply();
  }
  else if (this.state === FSM_CANDIDATE && this.voted.size + 1 < this.majority) {
    /* raft extension: prevent galloping term increments
       we are partitioned so wait until at least majority reponds with anything
       they will respond and quick if they ever come back (RpcSocket makes sure of that) */
    debug("ignoring vote request, regardless of peer's term, not enough vote responses");
    voteGranted = false;
    sendReply();
    return;
  }
  else {
                  // §5.2 we didn't vote in this term or we voted for the candidate already or higher term
    voteGranted = (!votedFor || votedFor === candidateId || candidateTerm > currentTerm)
               && // §5.4.1 candidate log is up-to-date
                  (candidateLastTerm  >  log.lastTerm ||
                  (candidateLastTerm === log.lastTerm && candidateLastIndex >= log.lastIndex));

    debug('vote granted: %s cand term: %s  my term: %s', voteGranted, candidateTerm, currentTerm);
    if (voteGranted || candidateTerm > currentTerm) {
      this.followLeaderId = null;
      this._refreshElectionTimeout();
      votedFor = voteGranted ? candidateId : null;
      currentTerm = candidateTerm;
      this._updateState(FSM_FOLLOWER, currentTerm, votedFor)
      .then(sendReply)
      .catch(err => this.emit('error', err));
    }
    else sendReply();
  }
}

function appendEntryHandler(reply, leaderId, leaderTerm, leaderPrevIndex, leaderPrevTerm, leaderCommit, ...entries) {
  if (!this.peers.has(leaderId)) {
    debug('append entry rpc: no such peer: %s', leaderId);
    return;
  }

  const debounceCache = bounceDuplicateRequestCache(this._rpcLastRequestIds, leaderId, reply);
  if (debounceCache === undefined) {
    debug('bounce duplicate append entry: %s', leaderId);
    return;    
  }

  const log = this._log;
  var currentTerm = this.currentTerm
    , votedFor = this.votedFor
    , success, conflictTerm, conflictTermIndex;

  const sendReply = () => reply(debounceCache.lastReplied = encodeAppendEntriesResponse([
    currentTerm, success, conflictTerm, conflictTermIndex
  ]));

  // §5.1
  if (leaderTerm < currentTerm) {
    success = false;
    sendReply();
  }
  else {
    this._clearElectionTimeout();
    this._router.pause();
    // §5.1
    this.followLeaderId = leaderId;
    // §5.3
    if (leaderTerm > currentTerm) {
      currentTerm = leaderTerm;
      votedFor = null;
    }
    Promise.all([
      log.termAt(leaderPrevIndex),
      this._updateState(FSM_FOLLOWER, currentTerm, votedFor)
    ])
    .then(([term]) => {
      if (term === leaderPrevTerm) {
        success = true;
        return log.appendEntries(entries, leaderPrevIndex + 1)
        .then(() => {
          if (leaderCommit > this.commitIndex) {
            this.commitIndex = min(leaderCommit, log.lastIndex);
          }
        });
      }
      else {
        success = false;
        debug("can't append entries: %s entries after: %s term: %s leaderTerm: %s", entries.length, leaderPrevIndex, term, leaderPrevTerm);
        if (log.lastIndex < leaderPrevIndex) {
          conflictTermIndex = log.lastIndex;
          conflictTerm = log.lastTerm;
        }
        // return log.firstIndexOfTerm(term)
        // .then(index => {
        //   conflictTerm = term;
        //   conflictTermIndex = index;
        // });
      }
    })
    .then(() => {
      sendReply();
      this._refreshElectionTimeout();
      this._router.resume();
      if (this.commitIndex > this.lastApplied) this._applyToStateMachine();
    })
    .catch(err => this.emit('error', err));
  }
}

function installSnapshotHandler(reply, leaderId, leaderTerm, lastIncludedIndex, lastIncludedTerm, position, dataSize, data) {
  if (!this.peers.has(leaderId)) {
    debug('install snapshot rpc: no such peer: %s', leaderId);
    return;
  }

  const debounceCache = bounceDuplicateRequestCache(this._rpcLastRequestIds, leaderId, reply);
  if (debounceCache === undefined) {
    debug('bounce duplicate install snapshot: %s', leaderId);
    return;    
  }

  const log = this._log;

  var currentTerm = this.currentTerm
    , votedFor = this.votedFor
    , positionRequest

  const sendReply = () => reply(debounceCache.lastReplied = encodeInstallSnapshotResponse(
    [currentTerm, positionRequest]
  ));

  // §5.1, §7
  if (leaderTerm < currentTerm) {
    sendReply();
  }
  else {
    this._clearElectionTimeout();
    this._router.pause();
    // §5.1
    this.followLeaderId = leaderId;
    // §5.3
    if (leaderTerm > currentTerm) {
      currentTerm = leaderTerm;
      votedFor = null;
    }

    var snapshot = this._tmpSnapshot;
    if (snapshot === null
        || snapshot.logIndex !== lastIncludedIndex
        || snapshot.logTerm !== lastIncludedTerm
        || snapshot.dataSize !== dataSize) {

      snapshot && snapshot.ready().then(s => s.close());
      snapshot = this._tmpSnapshot = log.createTmpSnapshot(lastIncludedIndex, lastIncludedTerm, dataSize);
      this._tmpSnapshotBytesWritten = 0;
    }

    Promise.all([
      snapshot.ready(),
      this._updateState(FSM_FOLLOWER, currentTerm, votedFor)
    ])
    .then(() => {
      if (this._tmpSnapshotBytesWritten !== position) {
        /* missing request perhaps */
        debug('install snapshot position <> written: %s <> %s', position, this._tmpSnapshotBytesWritten);
        positionRequest = this._tmpSnapshotBytesWritten;
      }
      else {
        return snapshot.write(data, position, data.length)
        .then(bytesWritten => {
          var total = this._tmpSnapshotBytesWritten = position + bytesWritten;
          if (total === dataSize) {
            /* finished writing snapshot */
            this._tmpSnapshot = null;
            return log.installSnapshot(snapshot);
          }
        });
      }
    })
    .then(() => {
      sendReply();
      this._refreshElectionTimeout();
      this._router.resume();
    })
    .catch(err => this.emit('error', err));
  }
}
