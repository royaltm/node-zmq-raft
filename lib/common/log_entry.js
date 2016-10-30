/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isBuffer = Buffer.isBuffer
    , isEncoding = Buffer.isEncoding
    , now = Date.now;

const { decode } = require('msgpack-lite');

const { assertConstantsDefined, defineConst } = require('../utils/helpers');

const REQUEST_LOG_ENTRY_OFFSET = 0
    , TYPE_LOG_ENTRY_OFFSET = REQUEST_LOG_ENTRY_OFFSET + 12
    , TERM_LOG_ENTRY_OFFSET = TYPE_LOG_ENTRY_OFFSET + 1
    , LOG_ENTRY_HEADER_SIZE = TERM_LOG_ENTRY_OFFSET + 7;

const LOG_ENTRY_TYPE_STATE      = 0
    , LOG_ENTRY_TYPE_CONFIG     = 1
    , LOG_ENTRY_TYPE_CHECKPOINT = 2;

const { REQUEST_UPDATE_TTL
      , REQUEST_UPDATE_IN_PAST_DELTA_MAX
      , REQUEST_UPDATE_IN_FUTURE_DELTA_MAX } = require('../common/constants');

assertConstantsDefined({
  REQUEST_UPDATE_TTL
, REQUEST_UPDATE_IN_PAST_DELTA_MAX
, REQUEST_UPDATE_IN_FUTURE_DELTA_MAX
}, 'number');

const { getSeconds } = require('../utils/id');

const isRequestExpired = (requestId) => (getSeconds(requestId) * 1000 < now() - REQUEST_UPDATE_TTL);
const checkRequestSanity = (requestId) => {
  const nowtime = now()
      , reqtime = getSeconds(requestId) * 1000;
  if (reqtime < nowtime - REQUEST_UPDATE_IN_PAST_DELTA_MAX){
    return -1;
  }
  else if (reqtime > nowtime + REQUEST_UPDATE_IN_FUTURE_DELTA_MAX) {
    return 1;
  }
  else return 0;
};

class LogEntry {
  constructor(entry, customDecode) {
    this.decode = customDecode || decode;
    if (!isBuffer(entry) || entry.length < LOG_ENTRY_HEADER_SIZE) {
      throw new Error("LogEntry: provided entry argument is not an entry");
    }
    this.entry = entry;
  }

  get type() {
    return defineConst(this, 'type', this.readType());
  }

  get isState() {
    return this.readType() === LOG_ENTRY_TYPE_STATE;
  }

  get isConfig() {
    return this.readType() === LOG_ENTRY_TYPE_CONFIG;
  }

  get isCheckpoint() {
    return this.readType() === LOG_ENTRY_TYPE_CHECKPOINT;
  }

  get isRequestExpired() {
    return isRequestExpired(this.requestId);
  }

  readType() {
    return this.entry[TYPE_LOG_ENTRY_OFFSET];
  }

  get term() {
    return defineConst(this, 'term', this.readTerm());
  }

  readTerm() {
    return this.entry.readUIntLE(TERM_LOG_ENTRY_OFFSET, 7, true);
  }

  get requestId() {
    return defineConst(this, 'requestId', this.readRequestId());
  }

  readRequestId() {
    return this.entry.slice(REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_OFFSET + 12);
  }

  get requestKey() {
    return defineConst(this, 'requestKey', this.readRequestKey());
  }

  readRequestKey(encoding) {
    encoding = isEncoding(encoding) ? encoding : 'hex'
    return this.entry.toString(encoding, REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_OFFSET + 12);
  }

  decodeData() {
    return this.decode(this.data);
  }

  get data() {
    return defineConst(this, 'data', this.readData());
  }

  readData() {
    return this.entry.slice(LOG_ENTRY_HEADER_SIZE);
  }

}

LogEntry.LogEntry = LogEntry;
LogEntry.readers = readers;
LogEntry.isRequestExpired = isRequestExpired;
LogEntry.REQUEST_LOG_ENTRY_OFFSET = REQUEST_LOG_ENTRY_OFFSET;
LogEntry.TYPE_LOG_ENTRY_OFFSET = TYPE_LOG_ENTRY_OFFSET;
LogEntry.TERM_LOG_ENTRY_OFFSET = TERM_LOG_ENTRY_OFFSET;
LogEntry.LOG_ENTRY_HEADER_SIZE = LOG_ENTRY_HEADER_SIZE;
LogEntry.LOG_ENTRY_TYPE_STATE      = LOG_ENTRY_TYPE_STATE;
LogEntry.LOG_ENTRY_TYPE_CONFIG     = LOG_ENTRY_TYPE_CONFIG;
LogEntry.LOG_ENTRY_TYPE_CHECKPOINT = LOG_ENTRY_TYPE_CHECKPOINT;

module.exports = exports = LogEntry;

var readers = LogEntry.readers = { readTypeOf, readTermOf, readRequestIdOf, readDataOf };

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
  return entry.readUIntLE(TERM_LOG_ENTRY_OFFSET, 7, true);
}

function readRequestIdOf(entry, encoding) {
  if (!isBuffer(entry) || entry.length < LOG_ENTRY_HEADER_SIZE) {
    throw new Error("readRequestIdOf: provided entry argument is not an entry");
  }
  return isEncoding(encoding)
          ? entry.toString(encoding, REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_OFFSET + 12)
          : entry.slice(REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_OFFSET + 12);
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
