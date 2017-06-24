/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const now = Date.now
    , min = Math.min

const assert = require('assert');
const zmq = require('zmq');

const debug = require('debug')('zmq-raft:server');

const { assertConstantsDefined } = require('../utils/helpers');

const { shared: lockShared } = require('../utils/lock');

const { checkRequestSanity } = require('../common/log_entry');

const { MAX_LOG_ENTRY_DATA_SIZE
      , REQUEST_ENTRIES_HIGH_WATERMARK
      , REQUEST_ENTRIES_TTL
      , REQUEST_ENTRIES_PIPELINES

      , RE_STATUS_NOT_LEADER
      , RE_STATUS_LAST
      , RE_STATUS_MORE
      , RE_STATUS_SNAPSHOT

      , FSM_LEADER
      } = require('../common/constants');

assertConstantsDefined({
  MAX_LOG_ENTRY_DATA_SIZE
, REQUEST_ENTRIES_HIGH_WATERMARK
, REQUEST_ENTRIES_TTL
, REQUEST_ENTRIES_PIPELINES
, RE_STATUS_NOT_LEADER
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
  encodeResponse: encodeRequestEntriesResponse } = createFramesProtocol('RequestEntries');
const {
  encodeResponse: encodeRequestConfigResponse }  = createFramesProtocol('RequestConfig');
const {
  encodeResponse: encodeRequestLogInfoResponse } = createFramesProtocol('RequestLogInfo');

/* client handlers */

exports.requestUpdateHandler = function(reply, logData) {
  const requestId = reply.requestId

  if (logData.length === 0 || logData.length > MAX_LOG_ENTRY_DATA_SIZE) {
    debug('invalid request data in request update, ignoring');
    return;    
  }

  if (requestId.length !== 12) {
    debug('invalid request id in request update, ignoring');
    return;
  }

  var success, respArg;
  const sendReply = () => reply(encodeRequestUpdateResponse([success, respArg]));

  const requestSanity = checkRequestSanity(requestId);
  if (requestSanity !== 0) {
    if (requestSanity > 0) debug('request update id is too far in the future');
    else debug('request update id is too far in the past');
    success = false;
    return sendReply();
  }

  if (this.state !== FSM_LEADER) {
    success = false;
    respArg = this.followLeaderId;
    return sendReply();
  }

  const log = this._log
      , requestKey = log.decodeRequestId(requestId)
      , currentTerm = this.currentTerm
      , updateRequests = this.updateRequests;

  success = true;

  /* double request, perhaps some net lag? */
  if (updateRequests.has(requestKey)) {
    debug('repeated request: [%s] replying with accept', requestKey);
    return sendReply();
  }

  /* check the request history in the log */
  const index = log.getRid(requestKey);

  /* request seen the 1st time */
  if (index === undefined) {
    let replyEntry = [reply, null];
    /* just mark in case of repeated request before we append to the log */
    updateRequests.set(requestKey, replyEntry);
    debug('appending entry term: %s [%s]', currentTerm, requestKey);
    sendReply(); /* send accept early */
    log.appendState(requestId, currentTerm, logData)
    .then(index => {
      if (currentTerm === this.currentTerm && this.state === FSM_LEADER) { /* sanity check */
        if (index <= this.lastApplied) {
          /* request already applied, almost impossible (too fast to round-trip),
             but another sanity check anyway */
          debug('appended update [%s] to log and commited at index: %s', requestKey, index);
          updateRequests.delete(requestKey);
          respArg = index;
          sendReply();
        } else {
          debug('appended update [%s] to log at index: %s', requestKey, index);
          replyEntry[1] = index;
          this._updateFollowersNow();
        }
      }
      else debug('leader stepped down before replying to a client');
    })
    .catch(err => this.error(err));
  }
  /* request seen and already applied  */
  else if (index <= this.lastApplied) {
    debug('request update: [%s] index: %s already applied', requestKey, index);
    respArg = index;
    sendReply();
  }
  /* request already appended but waiting for the majority to be applied */
  else {
    updateRequests.set(requestKey, [reply, index]);
    debug('request update: [%s] index %s in log but not yet applied', requestKey, index);
    sendReply();
  }
};


exports.requestEntriesHandler = function(reply, prevIndex, count) {

  var sendReply = (status, lastIndex, entries, byteOffset, snapshotSize) => {
    var arg1 = byteOffset !== undefined ? [byteOffset, snapshotSize]
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
  if (pending !== undefined) {
    pending.request(sendReply, prevIndex, count);
    return;
  }

  if (count === 0) {
    /* count == 0 indicates that no more entries should be send,
      so we'll ignore it since there is currently no pending streaming
      this behaviour is also ok for snapshots */
    return;
  }

  if (entriesRequests.size > REQUEST_ENTRIES_HIGH_WATERMARK) {
    debug('too many entries requests, dropping');
    return;
  }

  const log = this._log
      , lastApplied = this.lastApplied;

  var nextIndex = prevIndex + 1;

  if (nextIndex < log.firstIndex) {
    return lockAndSendSnapshot.call(this, sendReply, requestKey);
  }

  /* set lastIndex to the last index we will send */
  if (count === undefined || nextIndex + count - 1 > lastApplied) {
    count = lastApplied - prevIndex;
  }

  const lastIndex = nextIndex + count - 1;

  if (nextIndex > lastIndex) {
    return sendReply(RE_STATUS_LAST, lastIndex);
  }

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
    if (stream && queue.length > nextToSendIdx) {
      /* push back stream */
      // debug('push back stream');
      stream.pause();
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
    } else {
      firstQueuedIndex += entries.length;
      if (prevIndex !== firstQueuedIndex - 1) {
        /* we should cancel this request, cause transfer was lost, client is buggy or WTF */
        debug("request entries confirmation out of order: prevIndex: %s lastIndex: %s firstQueuedIndex: %s", prevIndex, lastIndex, firstQueuedIndex);
        cancel();
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

function lockAndSendSnapshot(sendReply, requestKey) {
  var snapshot = this._log.snapshot;
  return lockShared(snapshot, () => {
    if (snapshot.isClosed) {
      /* the log was compacted, we need the new snapshot */
      debug('snapshot closed, trying again');
      return lockAndSendSnapshot.call(this, sendReply, requestKey);
    }
    else {
      return new Promise((resolve, reject) => {
        sendSnapshot.call(this, sendReply, requestKey, snapshot, resolve);
      });
    }
  });
}

function sendSnapshot(sendReply, requestKey, snapshot, unlockSnapshot) {
  const logIndex = snapshot.logIndex
      , dataSize = snapshot.dataSize
      , entriesRequests = this.entriesRequests;

  const queue = [];
  var nextToSendIdx = 0;
  var byteOffset = 0;
  var stream = snapshot.createDataReadStream();

  const pipe = () => {
    while (nextToSendIdx < REQUEST_ENTRIES_PIPELINES && nextToSendIdx < queue.length) {
      var chunk = queue[nextToSendIdx++];
      sendReply(RE_STATUS_SNAPSHOT, logIndex, chunk, byteOffset, dataSize);
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

  const request = (installSendReply, prevIndex, count) => {
    sendReply = installSendReply;
    var chunk = queue.shift();
    --nextToSendIdx;
    if (count === 0) {
      /* client wants to stop streaming chunk */
      debug("request entries (snapshot) aborted by client request");
      cancel();
    }
    else if (chunk === undefined || prevIndex !== logIndex) {
      /* we should cancel this request, cause transfer was lost, client is buggy or WTF */
      debug("entries reply snapshot index incorrect: prevIndex: %s logIndex: %s", prevIndex, logIndex);
      cancel();
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
      , leaderId = isLeader ? this.myId : this.followLeaderId
      , peers = this.peersAry;

  reply(encodeRequestConfigResponse([isLeader, leaderId, peers]));
}

exports.requestLogInfoHandler = function(reply) {
  const isLeader = this.state === FSM_LEADER
      , log = this._log
      , leaderId = isLeader ? this.myId : this.followLeaderId;

  reply(encodeRequestLogInfoResponse([
    isLeader,
    leaderId,
    this.currentTerm,
    log.firstIndex,
    this.lastApplied,
    this.commitIndex,
    log.lastIndex,
    log.snapshot.dataSize,
  ]));
}
