/* 
 *  Copyright (c) 2016-2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const now = Date.now
    , min = Math.min

const assert = require('assert');

const debug = require('debug')('zmq-raft:server');

const { encode: encodeMsgPack } = require('@royaltm/msgpack-lite');

const { assertConstantsDefined } = require('../utils/helpers');

const { shared: lockShared } = require('../utils/lock');

const emptyChunk = Buffer.alloc(0);

const unhandled$ = Symbol('unhandled$');

const { RE_STATUS_NOT_LEADER
      , RE_STATUS_LAST
      , RE_STATUS_MORE
      , RE_STATUS_SNAPSHOT

      , FSM_LEADER
      } = require('../common/constants');

assertConstantsDefined({
  RE_STATUS_NOT_LEADER
, RE_STATUS_LAST
, RE_STATUS_MORE
, RE_STATUS_SNAPSHOT
}, 'number');

assertConstantsDefined({
  FSM_LEADER
}, 'symbol');

const { createFramesProtocol } = require('../protocol');

const {
  encodeResponse: encodeRequestUpdateResponse }  = createFramesProtocol('RequestUpdate');
const {
  encodeResponse: encodeConfigUpdateResponse }   = createFramesProtocol('ConfigUpdate');
const {
  encodeResponse: encodeRequestEntriesResponse } = createFramesProtocol('RequestEntries');
const {
  encodeResponse: encodeRequestConfigResponse }  = createFramesProtocol('RequestConfig');
const {
  encodeResponse: encodeRequestLogInfoResponse } = createFramesProtocol('RequestLogInfo');


/* client handlers */

exports.configUpdateHandler = function(reply, peers) {
  const requestId = reply.requestId
      , cluster = this.cluster;

  if (requestId.length !== 12) {
    debug('invalid request id in config update, ignoring');
    return;
  }

  const sendReply = (status, respArg) => reply(encodeConfigUpdateResponse([+status, respArg]));

  if (this.state !== FSM_LEADER) {
    return sendReply(0, this.followLeaderId);
  }

  const log = this._log
      , requestKey = log.decodeRequestId(requestId)
      , currentTerm = this.currentTerm;

  /* double request, perhaps some net lag? */
  if (this.configUpdateReply) {
    if (requestKey === this.peersUpdateRequest) {
      debug('repeated config update: [%s] replying with accept', requestKey);
      return sendReply(1);
    }
    else {
      debug('cluster membership change in transition');
      return sendReply(3);
    }
  }

  /* check the request history in the log */
  const index = log.getRid(requestKey);

  /* request seen the 1st time */
  if (index === undefined) {
    const requestSanity = this.checkRequestSanity(requestId);
    if (requestSanity !== 0) {
      if (requestSanity > 0) debug('config update id is too far in the future');
      else debug('config update id is too far in the past');

      return sendReply(4);
    }
    try {
      peers = cluster.join(peers);
    } catch(err) {
      debug('error parsing peers configuration: %s', err);
      return sendReply(2, {name: err.name, message: err.message});
    }

    if (peers === null) {
      debug('cluster membership change in transition');
      return sendReply(3);
    }

    /* just mark in case of repeated request before we append to the log */
    this.configUpdateReply = sendReply;
    debug('appending Cold,new term: %s [%s]', currentTerm, requestKey);
    sendReply(1); /* send accept early */
    this._router.pause();
    this._updateConfig(peers, requestKey, null);
    return this._persistence.update({peers, peersUpdateRequest: requestKey, peersIndex: null})
    .then(() => log.appendConfig(requestId, currentTerm, encodeMsgPack(peers)))
    .then(index => {
      this.peersIndex = index;
      const promise = this._persistence.update({peersIndex: index});
      debug('appended Cold,new [%s] to log at index: %s', requestKey, index);
      this._router.resume();
      if (currentTerm === this.currentTerm && this.state === FSM_LEADER) { /* sanity check */
        this._updateFollowersNow();
      } else debug('ConfigUpdate: leader stepped down before replicating Cold,new log entry');
      return promise;
    })
    .catch(err => this.error(err));
  }
  /* config seen and already committed  */
  else if (index <= this.commitIndex) {
    debug('config update: [%s] index: %s already applied', requestKey, index);
    sendReply(1, index);
  }
  /* config already appended but waiting for the majority to be applied */
  else if (requestKey === this.peersUpdateRequest) {
    this.configUpdateReply = sendReply;
    debug('config update: [%s] index %s in log but not yet applied', requestKey, index);
    sendReply(1);
  }
  else {
    debug('config update: [%s] index %s in log but not a config update', requestKey, index);
    return sendReply(2, {name: "Error", message: "wrong request id log entry type"});
  }
};

exports.requestUpdateHandler = function(reply, logData) {
  const requestId = reply.requestId;

  if (logData.length === 0 || logData.length > this.maxLogEntryDataSize) {
    debug('invalid request data in request update, ignoring');
    return;    
  }

  if (requestId.length !== 12) {
    debug('invalid request id in request update, ignoring');
    return;
  }

  const sendReply = (success, respArg) => reply(encodeRequestUpdateResponse([success, respArg]));

  if (this.state !== FSM_LEADER) {
    return sendReply(false, this.followLeaderId);
  }

  const log = this._log
      , requestKey = log.decodeRequestId(requestId)
      , currentTerm = this.currentTerm
      , updateRequests = this.updateRequests;

  /* double request, perhaps some net lag? */
  if (updateRequests.has(requestKey)) {
    debug('repeated request update: [%s] replying with accept', requestKey);
    return sendReply(true);
  }

  /* check the request history in the log */
  const index = log.getRid(requestKey);

  /* request seen for the 1st time */
  if (index === undefined) {
    const requestSanity = this.checkRequestSanity(requestId);
    if (requestSanity !== 0) {
      if (requestSanity > 0) debug('request update id is too far in the future');
      else debug('request update id is too far in the past');

      return sendReply(false);
    }
    let replyEntry = [sendReply, null];
    /* just mark in case of repeated request before we append to the log */
    updateRequests.set(requestKey, replyEntry);
    debug('appending state entry term: %s [%s]', currentTerm, requestKey);
    sendReply(true); /* send accept early */
    log.appendState(requestId, currentTerm, logData)
    .then(index => {
      if (currentTerm === this.currentTerm && this.state === FSM_LEADER) { /* sanity check */
        if (index <= this.lastApplied) {
          /* request already applied, almost impossible (too fast to round-trip),
             but another sanity check anyway */
          debug('appended update [%s] to log and commited at index: %s', requestKey, index);
          updateRequests.delete(requestKey);
          sendReply(true, index);
        } else {
          debug('appended update [%s] to log at index: %s', requestKey, index);
          replyEntry[1] = index;
          this._updateFollowersNow();
        }
      }
      else debug('RequestUpdate: leader stepped down before replying to a client');
    })
    .catch(err => this.error(err));
  }
  /* request seen and already applied  */
  else if (index <= this.lastApplied) {
    debug('request update: [%s] index: %s already applied', requestKey, index);
    sendReply(true, index);
  }
  /* request already appended but waiting for the majority to be applied */
  else {
    updateRequests.set(requestKey, [sendReply, index]);
    debug('request update: [%s] index %s in log but not yet applied', requestKey, index);
    sendReply(true);
  }
};


exports.requestEntriesHandler = function(reply, prevIndex, count, snapshotOffset) {

  var sendReply = (status, lastIndex, entries, byteOffset, snapshotSize, snapshotTerm) => {
    var arg1 = byteOffset !== undefined ? [byteOffset, snapshotSize, snapshotTerm]
                                        : this.followLeaderId;
    var payload = [status, arg1, lastIndex];
    if (entries !== undefined) payload = payload.concat(entries);
    reply(encodeRequestEntriesResponse(payload));
  };

  if (this.state !== FSM_LEADER) {
    return sendReply(RE_STATUS_NOT_LEADER);
  }

  const entriesRequests = this.entriesRequests;

  const requestKey = reply.ident.toString('base64') + reply.requestId.toString('base64');

  var pending = entriesRequests.get(requestKey);
  if (pending !== undefined &&
      unhandled$ !== pending.request(sendReply, prevIndex, count, snapshotOffset)) {
    return;
  }

  if (count === 0) {
    /* count === 0 indicates that no more entries should be send,
      so we'll ignore it since there is currently no pending streaming
      this behaviour is also ok for snapshots */
    return;
  }

  if (entriesRequests.size > this.requestEntriesHighWatermak) {
    debug('too many entries requests, dropping');
    return;
  }

  const log = this._log
      , lastApplied = this.lastApplied;

  var nextIndex = prevIndex + 1;

  if (nextIndex < log.firstIndex) {
    return lockAndSendSnapshot.call(this, sendReply, requestKey, snapshotOffset);
  }

  /* set lastIndex to the last index we will send */
  if (count == null || nextIndex + count - 1 > lastApplied) {
    count = lastApplied - prevIndex;
  }

  const lastIndex = nextIndex + count - 1;

  if (nextIndex > lastIndex) {
    return sendReply(RE_STATUS_LAST, lastIndex);
  }

  const REQUEST_ENTRIES_TTL = this.requestEntriesTtl;
  const REQUEST_ENTRIES_PIPELINES = this.requestEntriesPipelines;
  const REQUEST_ENTRIES_ENTRY_SIZE_LIMIT_PIPELINE = this.requestEntriesEntrySizeLimitPipeline;
  const queue = [];

  var nextToSendIdx = 0;
  var firstQueuedIndex = nextIndex;
  var stream = log.createEntriesReadStream(nextIndex, lastIndex);

  const pipe = () => {
    while (nextToSendIdx < REQUEST_ENTRIES_PIPELINES && nextToSendIdx < queue.length) {
      var entries = queue[nextToSendIdx++];
      prevIndex += entries.length;
      var status = prevIndex === lastIndex ? RE_STATUS_LAST : RE_STATUS_MORE;
      sendReply(status, prevIndex, entries);
      nextIndex = prevIndex + 1;
    }

    assert(prevIndex <= lastIndex);

    if (prevIndex === lastIndex && queue.length <= 1) {
      /* there should be no more confirmations, time to quit */
      if (entriesRequests.has(requestKey)) debug("request entries done");
      cancel();
    }
  };

  const ondata = (entries) => {
    queue.push(entries);
    pipe();
    if (stream) {
      if (queue.length > nextToSendIdx) {
        /* push back stream */
        // debug('push back stream');
        stream.pause();
      }
      else if (nextToSendIdx >= 1 && entries.length === 1 && entries[0].length > REQUEST_ENTRIES_ENTRY_SIZE_LIMIT_PIPELINE) {
        // debug('big entry, slowing down: %s', entries[0].length);
        stream.pause();
      }
    }
  };

  const onend = () => {
    stream = null;
  };

  const request = (installSendReply, prevIndex, count) => {
    sendReply = installSendReply;
    var entries = queue.shift();
    --nextToSendIdx;
    if (count === 0) {
      /* client wants to stop streaming entries */
      debug("request entries aborted by client request");
      cancel();
    }
    else if (entries === undefined) {
      /* we should cancel this request, cause transfer was lost, client is buggy or WTF */
      debug("request entries confirmation without entries in queue : prevIndex: %s lastIndex: %s firstQueuedIndex: %s", prevIndex, lastIndex, firstQueuedIndex);
      cancel();
      return unhandled$;
    } else {
      firstQueuedIndex += entries.length;
      if (prevIndex !== firstQueuedIndex - 1) {
        /* we should cancel this request, cause transfer was lost, client is buggy or WTF */
        debug("request entries confirmation out of order: prevIndex: %s lastIndex: %s firstQueuedIndex: %s", prevIndex, lastIndex, firstQueuedIndex);
        cancel();
        return unhandled$;
      }
      else {
        pipe();
        if (stream && queue.length <= nextToSendIdx) {
          /* pull stream again */
          // if (stream.isPaused) debug('pull stream again');
          stream.resume();
        }
        /* refresh this request */
        pending.expires = now() + REQUEST_ENTRIES_TTL;
      }
    }
  };

  const cancel = () => {
    entriesRequests.delete(requestKey);
    if (stream) {
      stream.cancel();
      stream.removeListener('data', ondata);
      stream.removeListener('end', onend);
      stream = null;
    }
  };

  stream.on('error', err => this.error(err))
        .on('data', ondata)
        .on('end', onend);

  /* create new request entries handler */
  pending = {
    expires: now() + REQUEST_ENTRIES_TTL,
    cancel: cancel,
    request: request
  };

  entriesRequests.set(requestKey, pending);

  ensureRequestEntriesTimeout.call(this);
}

function lockAndSendSnapshot(sendReply, requestKey, snapshotOffset) {
  var snapshot = this._log.snapshot;
  return lockShared(snapshot, () => {
    /* leader check - locking may take time */
    if (this.state !== FSM_LEADER) {
      return sendReply(RE_STATUS_NOT_LEADER);
    }
    else if (snapshot.isClosed) {
      /* the log was compacted, we need the new snapshot */
      debug('snapshot closed, trying again');
      return lockAndSendSnapshot.call(this, sendReply, requestKey, snapshotOffset);
    }
    else {
      return new Promise((resolve, reject) => {
        sendSnapshot.call(this, sendReply, requestKey, snapshot, resolve, snapshotOffset);
      });
    }
  });
}

function sendSnapshot(sendReply, requestKey, snapshot, unlockSnapshot, byteOffset) {
  const { logIndex, logTerm, dataSize } = snapshot
      , entriesRequests = this.entriesRequests;

  if (byteOffset === undefined) {
    byteOffset = 0;
  }
  else if (byteOffset > dataSize) {
    sendReply(RE_STATUS_SNAPSHOT, logIndex, emptyChunk, dataSize, dataSize, logTerm);
    return;
  }

  const REQUEST_ENTRIES_TTL = this.requestEntriesTtl;
  const REQUEST_ENTRIES_SNAPSHOT_PIPELINES = this.requestEntriesSnapshotPipelines;
  const queue = [];

  var nextToSendIdx = 0;
  var lastByteOffset = byteOffset;
  var stream = snapshot.createDataReadStream(byteOffset);

  const pipe = () => {
    while (nextToSendIdx < REQUEST_ENTRIES_SNAPSHOT_PIPELINES && nextToSendIdx < queue.length) {
      var chunk = queue[nextToSendIdx++];
      sendReply(RE_STATUS_SNAPSHOT, logIndex, chunk, byteOffset, dataSize, logTerm);
      byteOffset += chunk.length;
    }

    assert(byteOffset <= dataSize);

    if (byteOffset === dataSize && queue.length <= 1) {
      /* there should be no more confirmations, time to quit */
      if (entriesRequests.has(requestKey)) debug("sending snapshot done");
      cancel();
    }
  };

  const ondata = (chunk) => {
    queue.push(chunk);
    pipe();
    if (stream && queue.length > nextToSendIdx) {
      /* push back stream */
      // debug('push back stream');
      stream.pause();
    }
  };

  const onend = () => {
    stream = null;
    unlockSnapshot();
    pipe();
  };

  const request = (installSendReply, prevIndex, count, snapshotOffset) => {
    sendReply = installSendReply;
    var chunk = queue.shift();
    --nextToSendIdx;

    if (count === 0) {
      /* client wants to stop streaming chunk */
      debug("request entries (snapshot) aborted by client request");
      cancel();
    }
    else if (chunk === undefined || prevIndex >= logIndex) {
      /* we should cancel this request, cause transfer was lost, client is buggy or WTF */
      debug("entries reply snapshot index incorrect: prevIndex: %s logIndex: %s", prevIndex, logIndex);
      cancel();
      return unhandled$;
    }
    else {
      lastByteOffset += chunk.length;
      if (snapshotOffset !== undefined && snapshotOffset !== lastByteOffset) {
        /* we should cancel this request, cause transfer was lost, client is buggy or WTF */
        debug("entries reply snapshot byte offset not in order: next offset: %s, match offset: %s", snapshotOffset, lastByteOffset);
        cancel();
        return unhandled$;
      }
      else {
        pipe();
        if (stream && queue.length <= nextToSendIdx) {
          /* pull stream again */
          // if (stream.isPaused) debug('pull stream again');
          stream.resume();
        }
        /* refresh this request */
        pending.expires = now() + REQUEST_ENTRIES_TTL;
      }
    }
  };

  const cancel = () => {
    entriesRequests.delete(requestKey);
    if (stream) {
      stream.pause();
      stream.removeListener('data', ondata);
      stream.removeListener('end', onend);
      stream = null;
      unlockSnapshot();
    }
  };

  stream.on('error', err => this.error(err))
        .on('data', ondata)
        .on('end', onend);

  /* create new request entries handler */
  const pending = {
    expires: now() + REQUEST_ENTRIES_TTL,
    cancel: cancel,
    request: request
  };

  entriesRequests.set(requestKey, pending);

  ensureRequestEntriesTimeout.call(this);
}

function ensureRequestEntriesTimeout() {
  const entriesRequests = this.entriesRequests;
  /* periodically wipe them out */
  if (!this._entryRequestCleaner) {
    this._entryRequestCleaner = setInterval(() => {
      var time = now();
      for(var pending of entriesRequests.values()) {
        if (time > pending.expires) {
          debug("canceling expired request entries stream");
          pending.cancel();
        }
      }
      if (entriesRequests.size === 0) {
        clearInterval(this._entryRequestCleaner);
        this._entryRequestCleaner = null;
      }
    }, 2000);
    this._entryRequestCleaner.unref();
  }
}


exports.requestConfigHandler = function(reply) {
  const isLeader = this.state === FSM_LEADER
      , leaderId = isLeader ? this.peerId : this.followLeaderId
      , configAry = this.cluster.configAry;

  reply(encodeRequestConfigResponse([isLeader, leaderId, configAry]));
}

exports.requestLogInfoHandler = function(reply) {
  const isLeader = this.state === FSM_LEADER
      , log = this._log
      , leaderId = isLeader ? this.peerId
                            : this.followLeaderId;

  reply(encodeRequestLogInfoResponse([
    isLeader,
    leaderId,
    this.currentTerm,
    log.firstIndex,
    this.lastApplied,
    this.commitIndex,
    log.lastIndex,
    log.snapshot.dataSize,
    this.pruneIndex
  ]));
}
