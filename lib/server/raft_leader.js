/* 
 *  Copyright (c) 2016-2025 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const min = Math.min
   ,  push = Array.prototype.push;

const assert = require('assert');

const debug = require('debug')('zmq-raft:leader');

const { encode: encodeMsgPack } = require('msgpack-lite');

const { assertConstantsDefined } = require('../utils/helpers');
const { shared: lockShared } = require('../utils/lock');

const { generateRequestKey } = require('../common/log_entry');

const { FSM_CLIENT
      , FSM_FOLLOWER
      , FSM_CANDIDATE
      , FSM_LEADER

      , APPEND_ENTRY
      , REQUEST_VOTE
      , INSTALL_SNAPSHOT
      } = require('../common/constants');

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

const appendEntryTypeBuf     = Buffer.from(APPEND_ENTRY);
const requestVoteTypeBuf     = Buffer.from(REQUEST_VOTE);
const installSnapshotTypeBuf = Buffer.from(INSTALL_SNAPSHOT);

exports.updateFollower = updateFollower;
exports.becomeLeader = becomeLeader;

exports.onElectionTimeout = function() {

  const cluster = this.cluster;

 if (this.preventSpiralElections &&
     this.state === FSM_CANDIDATE && !cluster.majorityHasVoted()) {
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
      , votedFor = this.peerId;

  debug("election timeout term: %s", currentTerm);

  cluster.votingStart();
  cluster.vote(votedFor, true);

  this._updateState(FSM_CANDIDATE, currentTerm, votedFor)
  .then(() => {

    if (cluster.isSoleMaster) {
      this._router.resume();
      return becomeLeader.call(this);
    }

    const payload = encodeRequestVoteRequest([
      requestVoteTypeBuf,
      this._secretBuf,
      this.peerIdBuf,
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
  const cluster = this.cluster;
  if (term > this.currentTerm) { // §5.3
    cluster.vote(peerId, false);
    debug("vote response term: %s > %s", term, this.currentTerm);
    if (this.preventSpiralElections &&
        this.state === FSM_CANDIDATE && !cluster.majorityHasVoted()) {
      /* raft extension: prevent galloping term increments
         we are partitioned so wait until at least majority reponds with anything
         they will respond and quick if they ever come back (RpcSocket makes sure of that) */
      debug("ignoring vote response, regardless of peer's term, not enough vote responses");
      return;
    }
    else {
      return becomeFollower.call(this, term);
    }
  }
  else if (this.currentTerm === currentTerm && this.state === FSM_CANDIDATE) { // sanity check
    cluster.vote(peerId, voteGranted);
    debug("id: %s vote: %s yes: %s / %s  <?>  %s", peerId, voteGranted, cluster.votes, cluster.voted.size, cluster.majority);
    if (cluster.hasWonVoting()) {
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
      , nextIndex  = this.nextIndex
      , matchIndex = this.matchIndex
      , matchTerm  = this.matchTerm
      , slowHBeat  = this.slowHBeat
      , lastIndex = log.lastIndex
      , lastTerm  = log.lastTerm
      , commitIndex = this.commitIndex
      , currentTerm = this.currentTerm
      , payload = encodeAppendEntriesRequest([
        appendEntryTypeBuf,
        this._secretBuf,
        this.peerIdBuf,
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
    matchTerm.set(followerId, 0);
    slowHBeat.add(followerId);

    /*  Upon election: send initial emptyAppendEntries RPCs (heartbeat) to each server; */
    rpc.appendEntries(payload, this.appendEntriesRpcTimeoutMin)
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
  else if (this.cluster.isSoleMaster) {
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
    return becomeFollower.call(this, term);
  }
  else if (this.currentTerm === currentTerm && this.state === FSM_LEADER) { // sanity check
    if (success) {
      const matchIndex = this.matchIndex;
      followerNextIndex = sentLastIndex + 1;
      // • If successful: update nextIndex and matchIndex for follower (§5.3)
      matchIndex.set(followerId, sentLastIndex);
      this.matchTerm.set(followerId, sentLastTerm);
      this.nextIndex.set(followerId, followerNextIndex);
      // If there exists an N such that N > commitIndex, a majority
      // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
      // set commitIndex = N (§5.3, §5.4).
      if (sentLastTerm === currentTerm && sentLastIndex > this.commitIndex) {
        const cluster = this.cluster;
        if (cluster.majorityHasLogIndex(sentLastIndex, matchIndex)) {
          this.commitIndex = sentLastIndex;
          /* update all peers immediately with new commit index */
          this.slowHBeat.clear();
          this._updateFollowersNow();
          /* apply to state machine asynchronously */
          if (this.commitIndex > this.lastApplied) this._applyToStateMachine();
          // §6 Cluster membership changes
          if (!cluster.isMember(this.peerId)) {
            // §6 the leader steps down (returns to follower state)
            // once it has committed the Cnew log entry
            debug("no longer part of the cluster, stepping down");
            /* actually it will step down to FSM_CLIENT */
            return becomeFollower.call(this, term);
          }
          else if (cluster.isTransitional) {
            const peerIndexTransitional = this.peersIndex;
            if (peerIndexTransitional != null && this.commitIndex >= peerIndexTransitional) {
              // §6 Cold,new has been committed
              // It is now safe for the leader to create a log entry
              // describing Cnew and replicate it to the cluster.
              const peersUpdateRequest = generateRequestKey()
                  , peers = cluster.serializeNC();
              debug('appending Cnew term: %s [%s]', currentTerm, peersUpdateRequest);
              this._router.pause();
              this._updateConfig(peers, peersUpdateRequest, null);
              return this._persistence.update({peers, peersUpdateRequest, peersIndex: null})
              .then(() => this._log.appendConfig(peersUpdateRequest, currentTerm, encodeMsgPack(peers)))
              .then(peersIndex => {
                this.peersIndex = peersIndex;
                const promise = this._persistence.update({peersIndex});
                debug('appended Cnew [%s] to log at index: %s', peersUpdateRequest, peersIndex);
                this._router.resume();
                if (currentTerm === this.currentTerm && this.state === FSM_LEADER) { /* sanity check */
                  if (cluster.isMember(followerId)) {
                    updateFollower.call(this, followerId, followerNextIndex, success);
                  }
                  this._updateFollowersNow();
                  const configUpdateReply = this.configUpdateReply;
                  if (configUpdateReply) {
                    this.configUpdateReply = null;
                    configUpdateReply(1, peerIndexTransitional);
                  }
                } else debug('ConfigUpdate: leader stepped down before replicating Cnew log entry');
                return promise;
              });
            }
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
      this.nextIndex.set(followerId, followerNextIndex);
    }

    updateFollower.call(this, followerId, followerNextIndex, success);
  }
}

function updateFollower(followerId, followerNextIndex, prevSuccess) {
  var timeout = this._heartbeats.get(followerId),
      slowHBeat = this.slowHBeat;
  // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
  if ((this._log.lastIndex >= followerNextIndex) || !slowHBeat.has(followerId)) {
    if (timeout !== undefined) {
      /* clear sending heartbeat if any, will send update now */
      clearTimeout(timeout);
      this._heartbeats.delete(followerId);
    }
    slowHBeat.add(followerId);
    sendAppendEntries.call(this, followerId, followerNextIndex, prevSuccess);
  }
  /* repeat during idle periods to prevent election timeouts (§5.2) */
  else if (timeout == null) { // null timeout is used for new peers during transitional config
    this._heartbeats.set(followerId, setTimeout(() => {
      this._heartbeats.delete(followerId);
      sendAppendEntries.call(this, followerId, followerNextIndex, prevSuccess);
    }, this.appendEntriesHeartbeatInterval));
  }
}

function sendAppendEntries(followerId, followerNextIndex, manyEntriesAtOnce) {
  const log = this._log
      , currentTerm = this.currentTerm;

  /* there is no log before that, so send snapshot instead */
  if (followerNextIndex < log.firstIndex) {
    sendInstallSnapshot.call(this, currentTerm, followerId, log.snapshot, 0);
    return;
  }

  var timeout = this.appendEntriesRpcTimeoutMin
    , prevIndex = followerNextIndex - 1
    , prevTerm
    , lastIndex = prevIndex
    , lastTerm
    , termPromise;

  if (this.matchIndex.get(followerId) === prevIndex) {
    /* hot path */
    termPromise = Promise.resolve(this.matchTerm.get(followerId));
  }
  else {
    termPromise = log.termAt(prevIndex);
  }

  return termPromise.then(term => {

    prevTerm = lastTerm = term;

    const payload = [
      appendEntryTypeBuf,
      this._secretBuf,
      this.peerIdBuf,
      currentTerm,
      prevIndex,
      prevTerm,
      this.commitIndex
    ];

    if (prevIndex < log.lastIndex) {
      /* there are entries to append to followers */
      timeout = this.appendEntriesRpcTimeoutMax;

      const buffer = getOrCreateBuffer(this._followerBuffers, followerId, this.peerMsgDataSize);

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
    else {
      /* heartbeat only */
      return payload;
    }
  })
  .then(payload => {
    if (currentTerm !== this.currentTerm || this.state !== FSM_LEADER) {
      debug('leader stepped down before updating followers');
      return;
    }

    const rpc = this._rpcs.get(followerId);

    if (!rpc) {
      debug('follower %s is gone', followerId);
      return;
    }

    return rpc.appendEntries(payload, timeout)
    .then(args => onAppendEntriesResponse.call(this, currentTerm, followerId, prevIndex
                                                                            , lastIndex, lastTerm, ...args));
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

  const peerMsgDataSize = this.peerMsgDataSize;

  const length = min(dataSize - position, peerMsgDataSize);

  const buffer = getOrCreateBuffer(this._followerBuffers, followerId, peerMsgDataSize);

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
      /* do not return promise, we may unlock current snapshot now */
      sendInstallSnapshot.call(this, currentTerm, followerId, this._log.snapshot, 0);
      return;
    }

    assert(length === data.length);

    const rpc = this._rpcs.get(followerId);

    if (!rpc) {
      debug('follower %s is gone', followerId);
      return;
    }

    const done = position + length === dataSize;

    const payload = [
      installSnapshotTypeBuf,
      this._secretBuf,
      this.peerIdBuf,
      currentTerm,
      lastIncludedIndex,
      lastIncludedTerm,
      position,
      dataSize,
      data
    ];

    debug('install snapshot to %s size: %s offset: %s/%s index: %s term: %s', followerId, length, position, dataSize, lastIncludedIndex, lastIncludedTerm);

    return rpc.installSnapshot(payload, this.appendEntriesRpcTimeoutMax)
    .then(([term, reposition]) => {
      if (done && reposition === undefined) {
        return onAppendEntriesResponse.call(this, currentTerm, followerId, lastIncludedIndex
                                                                  , lastIncludedIndex, lastIncludedTerm
                                                                  , term, true);
      }
      else if (term > this.currentTerm) { // §5.3
        debug("install snapshot response term: %s > %s", term, this.currentTerm);
        return becomeFollower.call(this, term);
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
