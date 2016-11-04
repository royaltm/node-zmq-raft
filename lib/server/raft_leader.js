/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const min = Math.min
   ,  push = Array.prototype.push;

const assert = require('assert');

const zmq = require('zmq');
const debug = require('debug')('zmq-raft-leader');

const { assertConstantsDefined } = require('../utils/helpers');
const { shared: lockShared } = require('../utils/lock');

const { APPEND_ENTRIES_HEARTBEAT_INTERVAL
      , APPEND_ENTRIES_RPC_TIMEOUT_MIN
      , APPEND_ENTRIES_RPC_TIMEOUT_MAX

      , INSTALL_SNAPSHOT_SEND_DATA_MAX

      , FSM_CLIENT
      , FSM_FOLLOWER
      , FSM_CANDIDATE
      , FSM_LEADER

      , APPEND_ENTRY
      , REQUEST_VOTE
      , INSTALL_SNAPSHOT
      } = require('../common/constants');

const APPEND_ENTRIES_BUFFER_SIZE = INSTALL_SNAPSHOT_SEND_DATA_MAX; //TODO make configurable

assertConstantsDefined({
  APPEND_ENTRIES_BUFFER_SIZE
, APPEND_ENTRIES_HEARTBEAT_INTERVAL
, APPEND_ENTRIES_RPC_TIMEOUT_MIN
, APPEND_ENTRIES_RPC_TIMEOUT_MAX
, INSTALL_SNAPSHOT_SEND_DATA_MAX
}, 'number');

assertConstantsDefined({
  FSM_CLIENT
, FSM_FOLLOWER
, FSM_CANDIDATE
, FSM_LEADER
}, 'symbol');

assertConstantsDefined({
  APPEND_ENTRY
, REQUEST_VOTE
, INSTALL_SNAPSHOT
}, 'string', true);

const { createFramesProtocol } = require('../protocol');

const {encodeRequest: encodeRequestVoteRequest}   = createFramesProtocol('RequestVote');
const {encodeRequest: encodeAppendEntriesRequest} = createFramesProtocol('AppendEntries');
const {encodeRequest: encodeInstallSnapshotRequest} = createFramesProtocol('InstallSnapshot');

const appendEntryTypeBuf     = Buffer.from(APPEND_ENTRY);
const requestVoteTypeBuf     = Buffer.from(REQUEST_VOTE);
const installSnapshotTypeBuf = Buffer.from(INSTALL_SNAPSHOT);

exports.updateFollower = updateFollower;
exports.becomeLeader = becomeLeader;

exports.onElectionTimeout = function() {

  if (this.state === FSM_CANDIDATE && this.voted.size + 1 < this.majority) {
    /* raft extension: prevent galloping term increments
       we are partitioned so wait until at least majority reponds with anything
       they will respond and quick if they ever come back (RpcSocket makes sure of that) */
    this._refreshElectionTimeout();
    return;
  }

  // §5.2
  this._router.pause();

  this.followLeaderId = null;

  const log = this._log
      , currentTerm = this.currentTerm + 1
      , votedFor = this.myId;

  debug("election timeout term: %s", currentTerm);

  this.votes = 1;
  this.voted.clear();

  this._updateState(FSM_CANDIDATE, currentTerm, votedFor)
  .then(() => {
    const payload = encodeRequestVoteRequest([
      requestVoteTypeBuf,
      this._secretBuf,
      this.myIdBuf,
      currentTerm,
      log.lastIndex,
      log.lastTerm
    ]);

    for(let [peerId, rpc] of this._rpcs) {
      rpc.requestVote(payload)
      .then(args => onVoteResponse.call(this, currentTerm, peerId, ...args))
      .catch(err => {
        if (!err.isCancel) this.error(err);
      });
    }

    this._refreshElectionTimeout();
    this._router.resume();
  })
  .catch(err => this.error(err));
}

function onVoteResponse(currentTerm, peerId, term, voteGranted) {
  this.voted.add(peerId);
  const voted = this.voted.size + 1;
  if (term > this.currentTerm) { // §5.3
    debug("vote response term: %s > %s", term, this.currentTerm);
    if (this.state === FSM_CANDIDATE && voted < this.majority) {
      /* raft extension: prevent galloping term increments
         we are partitioned so wait until at least majority reponds with anything
         they will respond and quick if they ever come back (RpcSocket makes sure of that) */
      debug("ignoring vote response, regardless of peer's term, not enough vote responses");
      return;
    }
    return becomeFollower.call(this, term);
  }
  else if (this.currentTerm === currentTerm && this.state === FSM_CANDIDATE) { // sanity check
    let votes = voteGranted ? ++this.votes : this.votes;
    debug("id: %s vote: %s yes: %s / %s  <?>  %s", peerId, voteGranted, votes, voted, this.majority);
    if (votes >= this.majority) {
      return becomeLeader.call(this);
    }
  }
}

function becomeFollower(term) {
  this.followLeaderId = null;
  this._refreshElectionTimeout();
  return this._updateState(FSM_FOLLOWER, term, null);  
}

function becomeLeader() {
  // §5.2 become leader
  this._clearElectionTimeout();

  const log = this._log
      , nextIndex = this.nextIndex
      , matchIndex = this.matchIndex
      , lastIndex = log.lastIndex
      , lastTerm = log.lastTerm
      , commitIndex = this.commitIndex
      , currentTerm = this.currentTerm
      , payload = encodeAppendEntriesRequest([
        appendEntryTypeBuf,
        this._secretBuf,
        this.myIdBuf,
        currentTerm,
        lastIndex,
        lastTerm,
        commitIndex
      ]);

  debug("becoming -+*LEADER*+- index: %s term: %s", lastIndex, currentTerm);

  this.followLeaderId = null;
  this._updateState(FSM_LEADER, currentTerm, this.votedFor); /* no persistent state chages so ignore result */

  for(let [followerId, rpc] of this._rpcs) {
    /* re-initialized after election */
    nextIndex.set(followerId, lastIndex + 1);
    matchIndex.set(followerId, 0);
    /*  Upon election: send initial emptyAppendEntries RPCs (heartbeat) to each server; */
    rpc.appendEntries(payload, APPEND_ENTRIES_RPC_TIMEOUT_MIN)
    .then(args => onAppendEntriesResponse.call(this, currentTerm, followerId, lastIndex
                                                                            , lastIndex, lastTerm, ...args))
    .catch(err => {
      if (!err.isCancel) this.error(err);
    });
  }

  if (lastTerm !== currentTerm && commitIndex < lastIndex) {
    /* §5.4 we can't commit entries from previous terms so we'll create a new checkpoint log entry */
    debug('appending checkpoint to outstanging entries after commit: %s last: %s term: %s', commitIndex, lastIndex, currentTerm);
    return log.appendCheckpoint(currentTerm).then(() => this._updateFollowersNow());
  }
  else if (this.majority === 1) {
    this._updateFollowersNow();
  }
}

function onAppendEntriesResponse(currentTerm, followerId, sentPrevIndex
                                                        , sentLastIndex, sentLastTerm
                                                        , term, success, conflictTermIndex, conflictTerm) {
  var followerNextIndex;

  // debug('append entries response: %s sentPrevIndex: %s sentPrevTerm: %s term: %s success: %s', followerId, sentPrevIndex, sentPrevTerm, term, success);

  /* §5.3
     yes, check against this.currentTerm (currentTerm from args may be stalled see: else if) */
  if (term > this.currentTerm) {
    debug("append entries response term: %s > current: %s", term, this.currentTerm);
    becomeFollower.call(this, term);
  }
  else if (this.currentTerm === currentTerm && this.state === FSM_LEADER) { // sanity check
    const matchIndex = this.matchIndex
        , nextIndex = this.nextIndex;

    if (success) {
      followerNextIndex = sentLastIndex + 1;
      // • If successful: update nextIndex and matchIndex for follower (§5.3)
      matchIndex.set(followerId, sentLastIndex);
      nextIndex.set(followerId, followerNextIndex);
      // If there exists an N such that N > commitIndex, a majority
      // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
      // set commitIndex = N (§5.3, §5.4).
      if (sentLastTerm === currentTerm && sentLastIndex > this.commitIndex) {
        let majority = this.majority;
        for(let id of this.peers.keys()) {
          if (matchIndex.get(id) >= sentLastIndex && (--majority) === 1) {
            this.commitIndex = sentLastIndex;
            if (this.commitIndex > this.lastApplied) this._applyToStateMachine();
            break;
          }
        }        
      }
    }
    else {
      followerNextIndex = sentPrevIndex;
      // • IfAppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
      // TODO: handle conflictTerm and conflictTermIndex better
      if (conflictTermIndex !== undefined && conflictTermIndex < followerNextIndex) {
        debug("conflictTermIndex: %s followerNextIndex: %s", conflictTermIndex, followerNextIndex);
        followerNextIndex = conflictTermIndex;
        if (followerNextIndex < this._log.firstIndex) followerNextIndex = this._log.firstIndex - 1;
      }
      nextIndex.set(followerId, followerNextIndex);
    }
    updateFollower.call(this, followerId, followerNextIndex, success);
  }

}

function updateFollower(followerId, followerNextIndex, prevSuccess) {
  var timeout = this._heartbeats.get(followerId);
  // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
  if (this._log.lastIndex >= followerNextIndex) {
    if (timeout !== undefined) {
      /* clear sending heartbeat if any, will send update now */
      clearTimeout(timeout);
      this._heartbeats.delete(followerId);
    }
    sendAppendEntries.call(this, followerId, followerNextIndex, prevSuccess);
  } 
  /* repeat during idle periods to prevent election timeouts (§5.2) */
  else if (timeout === undefined) {
    this._heartbeats.set(followerId, setTimeout(() => {
      this._heartbeats.delete(followerId);
      sendAppendEntries.call(this, followerId, followerNextIndex)
    }, APPEND_ENTRIES_HEARTBEAT_INTERVAL));
  }
}

function sendAppendEntries(followerId, followerNextIndex, manyEntriesAtOnce) {
  const log = this._log
      , currentTerm = this.currentTerm;

  /* there is no log before that, so send snapshot instead */
  if (followerNextIndex < log.firstIndex) {
    return sendInstallSnapshot.call(this, currentTerm, followerId, log.snapshot, 0);
  }

  var timeout = APPEND_ENTRIES_RPC_TIMEOUT_MIN
    , prevIndex = followerNextIndex - 1
    , prevTerm
    , lastIndex = prevIndex
    , lastTerm;

  return log.termAt(prevIndex).then(term => {

    prevTerm = lastTerm = term;

    const payload = [
      appendEntryTypeBuf,
      this._secretBuf,
      this.myIdBuf,
      currentTerm,
      prevIndex,
      prevTerm,
      this.commitIndex
    ];

    if (prevIndex < log.lastIndex) {
      /* there are entries to append to followers */
      timeout = APPEND_ENTRIES_RPC_TIMEOUT_MAX;

      const buffer = getOrCreateBuffer(this._followerBuffers, followerId, APPEND_ENTRIES_BUFFER_SIZE);

      if (manyEntriesAtOnce) {
        return log.readEntries(followerNextIndex, log.lastIndex, buffer)
        .then(entries => {
          const count = entries.length;
          lastIndex += count;
          lastTerm = log.readTermOf(entries[count - 1]);
          push.apply(payload, entries);
          debug('updating follower %s with entries: %s - %s term: %s prev: %s term: %s', followerId, followerNextIndex, lastIndex, lastTerm, prevIndex, prevTerm);
          return payload;
        });
      } else {
        return log.getEntry(followerNextIndex, buffer)
        .then(entry => {
          lastIndex += 1;
          lastTerm = log.readTermOf(entry);
          payload.push(entry);
          debug('updating follower %s with entry: %s term: %s prev: %s term: %s', followerId, lastIndex, lastTerm, prevIndex, prevTerm);
          return payload;
        });
      }
    }
    else return payload;
  })
  .then(payload => {
    if (currentTerm !== this.currentTerm || this.state !== FSM_LEADER) {
      debug('leader stepped down before updating followers');
      return;
    }

    const rpc = this._rpcs.get(followerId);

    if (!rpc || rpc.pending) return; // sanity check

    return rpc.appendEntries(payload, timeout)
    .then(args => onAppendEntriesResponse.call(this, currentTerm, followerId, prevIndex
                                                                            , lastIndex, lastTerm, ...args))
  })
  .catch(err => {
    if (!err.isCancel) this.error(err);
  });
}

function sendInstallSnapshot(currentTerm, followerId, snapshot, position) {
  const lastIncludedIndex = snapshot.logIndex
      , lastIncludedTerm = snapshot.logTerm
      , dataSize = snapshot.dataSize;

  if (position < 0) position = 0;
  if (position > dataSize) position = dataSize;
  const length = min(dataSize - position, INSTALL_SNAPSHOT_SEND_DATA_MAX);

  const buffer = getOrCreateBuffer(this._followerBuffers, followerId, INSTALL_SNAPSHOT_SEND_DATA_MAX);

  return lockShared(snapshot, () => {
    if (snapshot.isClosed) {
      /* the snapshot was compacted, we need to restart sending it */
      return null;
    }
    else return snapshot.read(position, length, buffer, 0);
  })
  .then(data => {
    if (currentTerm !== this.currentTerm || this.state !== FSM_LEADER) return; // sanity check
    if (data === null) {
      /* snapshot was closed need to restart */
      debug('snapshot closed, need to start sending the new one');
      return sendInstallSnapshot.call(this, currentTerm, followerId, this._log.snapshot, 0);
    }

    assert(length === data.length);

    const rpc = this._rpcs.get(followerId);

    if (!rpc || rpc.pending) return; // sanity check

    const done = position + length === dataSize;

    const payload = [
      installSnapshotTypeBuf,
      this._secretBuf,
      this.myIdBuf,
      currentTerm,
      lastIncludedIndex,
      lastIncludedTerm,
      position,
      dataSize,
      data
    ];

    debug('install snapshot to %s size: %s offset: %s/%s index: %s term: %s', followerId, length, position, dataSize, lastIncludedIndex, lastIncludedTerm);

    return rpc.installSnapshot(payload, APPEND_ENTRIES_RPC_TIMEOUT_MAX)
    .then(([term, reposition]) => {
      if (done && reposition === undefined) {
        onAppendEntriesResponse.call(this, currentTerm, followerId, lastIncludedIndex
                                                                  , lastIncludedIndex, lastIncludedTerm
                                                                  , term, true);
      }
      else if (term > this.currentTerm) { // §5.3
        debug("install snapshot response term: %s > %s", term, this.currentTerm);
        becomeFollower.call(this, term);
      }
      else if (this.currentTerm === currentTerm && this.state === FSM_LEADER) { // sanity check
        if (reposition === undefined) {
          reposition = position + length;
        }
        sendInstallSnapshot.call(this, currentTerm, followerId, snapshot, reposition);
      }
      else {
        debug('leader stepped down before sending snapshot to followers');
      }
    })
    .catch(err => {
      if (!err.isCancel) throw err;
    });
  })
  .catch(err => this.error(err));
}


function getOrCreateBuffer(map, id, minlength) {
  var buffer = map.get(id);
  if (buffer === undefined || buffer.length < minlength) {
    buffer = Buffer.allocUnsafeSlow(minlength);
    map.set(id, buffer);
  }
  return buffer;
}
