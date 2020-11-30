/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const setPrototypeOf = Object.setPrototypeOf
    , isBuffer = Buffer.isBuffer
    , isArray = Array.isArray
    , isEncoding = Buffer.isEncoding
    , now = Date.now;

const { defineConst } = require('../utils/helpers');

const { writeBufUIntLE, readBufUIntLE } = require('../utils/bufconv');

const REQUEST_LOG_ENTRY_OFFSET = 0
    , REQUEST_LOG_ENTRY_LENGTH = 12
    , REQUEST_LOG_ENTRY_HEX_LENGTH = 24
    , REQUEST_LOG_ENTRY_BASE64_LENGTH = 16
    , TYPE_LOG_ENTRY_OFFSET = REQUEST_LOG_ENTRY_OFFSET + REQUEST_LOG_ENTRY_LENGTH
    , TERM_LOG_ENTRY_OFFSET = TYPE_LOG_ENTRY_OFFSET + 1
    , LOG_ENTRY_HEADER_SIZE = TERM_LOG_ENTRY_OFFSET + 7;

const LOG_ENTRY_TYPE_STATE      = 0
    , LOG_ENTRY_TYPE_CONFIG     = 1
    , LOG_ENTRY_TYPE_CHECKPOINT = 2;

const { getSeconds, genIdent, isIdent } = require('../utils/id');

const generateRequestKey = () => genIdent('base64');
const generateRequestId = () => genIdent('buffer');

/**
 * Creates a function that returns true if the given requestId has expired,
 * otherwise the function returns false.
 *
 * @param {number|null} requestIdTtl - request ID Time-To-Live in milliseconds;
 *               if requestIdTtl is null any requestId will be considered fresh.
 * @return {function(requestId: string|Buffer, offset?: number)}
**/
function makeHasRequestExpired(requestIdTtl) {
  if (requestIdTtl == null) {
    return function hasRequestExpired(requestId, offset) { return false }
  }
  else {
    return function hasRequestExpired(requestId, offset) {
      return getSeconds(requestId, offset) * 1000 < now() - requestIdTtl;
    };
  }
}

/**
 * Creates a function that returns 0 if the given requestId should be accepted.
 * Otherwise the function returns -1 if the given requestId is too old, and 1
 * if the given requestId is from the far future.
 *
 * @param {number|null} requestIdTtl - request ID Time-To-Live in milliseconds;
 *                       if requestIdTtl is null any requestId will be accepted.
 * @param {number} msDiffMarginPast - a margin Time-To-Live value in milliseconds;
 *                                 the requestIdTtl will be decresed by this value.
 * @param {number} msDiffMarginFuture - a future margin time-to-live value in milliseconds.
 * @return {function(requestId: string|Buffer, offset?: number)}
**/
function makeCheckRequestSanity(requestIdTtl, msDiffMarginPast, msDiffMarginFuture) {
  if (requestIdTtl == null) {
    return function checkRequestSanity(requestId, offset) { return 0 };
  }
  else {
    const REQUEST_ID_IN_PAST_DELTA_MAX = requestIdTtl - msDiffMarginPast;
    const REQUEST_ID_IN_FUTURE_DELTA_MAX = msDiffMarginFuture;
    return function checkRequestSanity(requestId, offset) {
      const nowtime = now()
          , reqtime = getSeconds(requestId, offset) * 1000;
      if (reqtime < nowtime - REQUEST_ID_IN_PAST_DELTA_MAX){
        return -1;
      }
      else if (reqtime > nowtime + REQUEST_ID_IN_FUTURE_DELTA_MAX) {
        return 1;
      }
      else return 0;
    };
  }
}

class UpdateRequest extends Buffer {
  constructor(buffer, requestId) {
    if (!isBuffer(buffer)) {
      throw new TypeError("UpdateRequest: provided buffer argument is not a buffer");
    }
    var ureq = setPrototypeOf(new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength), UpdateRequest.prototype);
    ureq.requestId = requestId;
    return ureq;
  }

  static isUpdateRequest(buffer) {
    return isBuffer(buffer) && isIdent(buffer.requestId);
  }

  static bufferToUpdateRequest(buffer, requestId) {
    if (!isBuffer(buffer)) {
      throw new TypeError("UpdateRequest: provided buffer argument is not a buffer");
    }
    setPrototypeOf(buffer, UpdateRequest.prototype);
    buffer.requestId = requestId;
    return buffer;
  }
}

/* in node 4 and later Buffer hackishly descends from Uint8Array */
class LogEntry extends Buffer {
  /* Creates a new LogEntry instance from a buffer without copying its bytes */
  constructor(buffer, logIndex) {
    if (!isBuffer(buffer) || buffer.length < LOG_ENTRY_HEADER_SIZE) {
      throw new TypeError("LogEntry: provided buffer argument does not represent a log entry");
    }
    /* we can't call super directly, Buffer is not a direct descend of Uint8Array but rather a hack
       so we have to hack here */
    var entry = setPrototypeOf(new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength), LogEntry.prototype);
    entry.logIndex = logIndex;
    return entry;
  }

  static isLogEntry(entry) {
    return entry instanceof LogEntry;
  }

  /* Converts buffer instance to LogEntry (replaces its __proto__) */
  static bufferToLogEntry(buffer, logIndex) {
    if (!isBuffer(buffer) || buffer.length < LOG_ENTRY_HEADER_SIZE) {
      throw new TypeError("LogEntry: provided buffer argument is not a log entry");
    }
    setPrototypeOf(buffer, LogEntry.prototype);
    buffer.logIndex = logIndex;
    return buffer;
  }

  static build(requestId, type, term, data, logIndex) {
    var entry = Buffer.allocUnsafe(LOG_ENTRY_HEADER_SIZE + data.length);
    setPrototypeOf(entry, LogEntry.prototype);
    if (requestId === null) {
      entry.fill(0, REQUEST_LOG_ENTRY_OFFSET, TYPE_LOG_ENTRY_OFFSET);
    }
    else if (isBuffer(requestId) && requestId.length >= REQUEST_LOG_ENTRY_LENGTH) {
      requestId.copy(entry, REQUEST_LOG_ENTRY_OFFSET, 0, REQUEST_LOG_ENTRY_LENGTH);
    }
    else if ('string' === typeof requestId) {
      if (requestId.length === REQUEST_LOG_ENTRY_HEX_LENGTH) {
        entry.write(requestId, REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_LENGTH, 'hex');
      }
      else if (requestId.length === REQUEST_LOG_ENTRY_BASE64_LENGTH) {
        entry.write(requestId, REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_LENGTH, 'base64');
      }
      else throw new TypeError("LogEntry.build: requestId must be a proper size hex or base64 string");
    }
    else throw new TypeError("LogEntry.build: requestId must be a null, buffer or a string");
    entry[TYPE_LOG_ENTRY_OFFSET] = type;
    writeBufUIntLE(term, entry, TERM_LOG_ENTRY_OFFSET, LOG_ENTRY_HEADER_SIZE);
    if (isBuffer(data)) {
      data.copy(entry, LOG_ENTRY_HEADER_SIZE);
    }
    else if (isArray(data)) {
      for(var i = data.length; i-- > 0; entry[LOG_ENTRY_HEADER_SIZE + i] = data[i]);
    }
    entry.logIndex = logIndex;
    return entry;
  }

  get entryType() {
    return this[TYPE_LOG_ENTRY_OFFSET];
  }

  set entryType(type) {
    this[TYPE_LOG_ENTRY_OFFSET] = type;
  }

  get isStateEntry() {
    return this[TYPE_LOG_ENTRY_OFFSET] === LOG_ENTRY_TYPE_STATE;
  }

  get isConfigEntry() {
    return this[TYPE_LOG_ENTRY_OFFSET] === LOG_ENTRY_TYPE_CONFIG;
  }

  get isCheckpointEntry() {
    return this[TYPE_LOG_ENTRY_OFFSET] === LOG_ENTRY_TYPE_CHECKPOINT;
  }

  get requestId() {
    return this._requestId || (this._requestId = this.readRequestId());
  }

  readRequestId() {
    return this.slice(REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_OFFSET + REQUEST_LOG_ENTRY_LENGTH);
  }

  readRequestKey(encoding) {
    encoding = isEncoding(encoding) ? encoding : 'hex'
    return this.toString(encoding, REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_OFFSET + REQUEST_LOG_ENTRY_LENGTH);
  }

  readEntryTerm() {
    return readBufUIntLE(this, TERM_LOG_ENTRY_OFFSET, LOG_ENTRY_HEADER_SIZE);
  }

  writeEntryTerm(term) {
    writeBufUIntLE(term, this, TERM_LOG_ENTRY_OFFSET, LOG_ENTRY_HEADER_SIZE);
  }

  readEntryData() {
    return this.slice(LOG_ENTRY_HEADER_SIZE);
  }

}

defineConst(LogEntry.prototype, 'isLogEntry', true);
defineConst(LogEntry.prototype, 'entryDataOffset', LOG_ENTRY_HEADER_SIZE);

defineConst(LogEntry, 'UpdateRequest',             UpdateRequest);
defineConst(LogEntry, 'LogEntry',                  LogEntry);
defineConst(LogEntry, 'makeHasRequestExpired',     makeHasRequestExpired);
defineConst(LogEntry, 'generateRequestKey',        generateRequestKey);
defineConst(LogEntry, 'generateRequestId',         generateRequestId);
defineConst(LogEntry, 'makeCheckRequestSanity',    makeCheckRequestSanity);
defineConst(LogEntry, 'lastConfigOffsetOf',        lastConfigOffsetOf);
defineConst(LogEntry, 'REQUEST_LOG_ENTRY_OFFSET',  REQUEST_LOG_ENTRY_OFFSET);
defineConst(LogEntry, 'REQUEST_LOG_ENTRY_LENGTH',  REQUEST_LOG_ENTRY_LENGTH);
defineConst(LogEntry, 'REQUEST_LOG_ENTRY_HEX_LENGTH',  REQUEST_LOG_ENTRY_HEX_LENGTH);
defineConst(LogEntry, 'REQUEST_LOG_ENTRY_BASE64_LENGTH',  REQUEST_LOG_ENTRY_BASE64_LENGTH);
defineConst(LogEntry, 'TYPE_LOG_ENTRY_OFFSET',     TYPE_LOG_ENTRY_OFFSET);
defineConst(LogEntry, 'TERM_LOG_ENTRY_OFFSET',     TERM_LOG_ENTRY_OFFSET);
defineConst(LogEntry, 'LOG_ENTRY_HEADER_SIZE',     LOG_ENTRY_HEADER_SIZE);
defineConst(LogEntry, 'LOG_ENTRY_TYPE_STATE',      LOG_ENTRY_TYPE_STATE);
defineConst(LogEntry, 'LOG_ENTRY_TYPE_CONFIG',     LOG_ENTRY_TYPE_CONFIG);
defineConst(LogEntry, 'LOG_ENTRY_TYPE_CHECKPOINT', LOG_ENTRY_TYPE_CHECKPOINT);

function lastConfigOffsetOf(entries) {
  var i = entries.length;
  while(i--) {
    if (readTypeOf(entries[i]) === LOG_ENTRY_TYPE_CONFIG) return i;
  }
}

var readers = { readTypeOf, readTermOf, readRequestIdOf, readDataOf };

defineConst(LogEntry, 'readers', readers);

module.exports = exports = LogEntry;

function readTypeOf(entry) {
  if (!isBuffer(entry) || entry.length < LOG_ENTRY_HEADER_SIZE) {
    throw new Error("readTermOf: provided entry argument is not an entry");
  }
  return entry[TYPE_LOG_ENTRY_OFFSET];
}

function readTermOf(entry) {
  if (!isBuffer(entry) || entry.length < LOG_ENTRY_HEADER_SIZE) {
    throw new Error("readTermOf: provided entry argument is not an entry");
  }
  return readBufUIntLE(entry, TERM_LOG_ENTRY_OFFSET, LOG_ENTRY_HEADER_SIZE);
}

function readRequestIdOf(entry, encoding) {
  if (!isBuffer(entry) || entry.length < LOG_ENTRY_HEADER_SIZE) {
    throw new Error("readRequestIdOf: provided entry argument is not an entry");
  }
  return isEncoding(encoding)
          ? entry.toString(encoding, REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_OFFSET + REQUEST_LOG_ENTRY_LENGTH)
          : entry.slice(REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_OFFSET + REQUEST_LOG_ENTRY_LENGTH);
}

function readDataOf(entry) {
  if (!isBuffer(entry) || entry.length < LOG_ENTRY_HEADER_SIZE) {
    throw new Error("readTermOf: provided entry argument is not an entry");
  }
  return entry.slice(LOG_ENTRY_HEADER_SIZE);
}

LogEntry.mixinReaders = function(proto) {
  for(var key in readers) {
    proto[key] = readers[key];
  }
};
