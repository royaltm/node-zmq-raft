/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const { LogEntry
      , UpdateRequest
      , hasRequestExpired
      , checkRequestSanity
      , readers
      , mixinReaders
      , LOG_ENTRY_HEADER_SIZE } = raft.common.LogEntry;

test('should have functions and properties', t => {
  t.strictEquals(LOG_ENTRY_HEADER_SIZE, 20);
  t.type(LogEntry               , 'function');
  t.type(UpdateRequest          , 'function');
  t.type(hasRequestExpired      , 'function');
  t.type(checkRequestSanity     , 'function');
  t.type(readers.readTypeOf     , 'function');
  t.type(readers.readTermOf     , 'function');
  t.type(readers.readRequestIdOf, 'function');
  t.type(readers.readDataOf     , 'function');
  t.type(mixinReaders           , 'function');
  t.end();
});

test('readers', t => {
  t.strictEquals(readers.readTypeOf(Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,77,0,0,0,0,0,0,0])), 77);
  t.strictEquals(readers.readTermOf(Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,0,7,6,5,4,3,2,1])), 7+6*0x100+5*0x10000+4*0x1000000+3*0x100000000+2*0x10000000000+0x1000000000000);
  t.deepEquals(readers.readRequestIdOf(Buffer.from([1,2,3,4,5,6,7,8,9,10,11,12,0,0,0,0,0,0,0,0])), Buffer.from([1,2,3,4,5,6,7,8,9,10,11,12]));
  t.deepEquals(readers.readRequestIdOf(Buffer.from([1,2,3,4,5,6,7,8,9,10,11,12,0,0,0,0,0,0,0,0]), 'hex'), '0102030405060708090a0b0c');
  t.deepEquals(readers.readDataOf(Buffer.alloc(20)), Buffer.alloc(0));
  t.deepEquals(readers.readDataOf(Buffer.alloc(21)), Buffer.from([0]));
  t.end();
});

test('hasRequestExpired', t => {
  var req = Buffer.alloc(12);
  t.strictEquals(hasRequestExpired(req), true);
  t.strictEquals(hasRequestExpired('000000000000000000000000'), true);
  t.strictEquals(hasRequestExpired('ffffffffffffffffffffffff'), false);
  t.strictEquals(hasRequestExpired(Buffer.from('000000000000000000000000ffffffffffffffffffffffff','hex'), 12), false);
  t.strictEquals(hasRequestExpired(Buffer.from('ffffffffffffffffffffffff000000000000000000000000','hex'), 12), true);
  t.strictEquals(hasRequestExpired(raft.utils.id.genIdent('buffer')), false);
  t.strictEquals(hasRequestExpired(raft.utils.id.genIdent()), false);
  req.writeUInt32BE((Date.now() - raft.common.constants.REQUEST_UPDATE_TTL + 2000) / 1000 >>> 0, 0);
  t.strictEquals(hasRequestExpired(req), false);
  req.writeUInt32BE((Date.now() - raft.common.constants.REQUEST_UPDATE_TTL) / 1000 >>> 0, 0);
  t.strictEquals(hasRequestExpired(req), true);
  t.end();
});

test('checkRequestSanity', t => {
  var req = Buffer.alloc(12);
  t.strictEquals(checkRequestSanity(req), -1);
  t.strictEquals(checkRequestSanity('000000000000000000000000'), -1);
  t.strictEquals(checkRequestSanity(raft.utils.id.genIdent('buffer')), 0);
  t.strictEquals(checkRequestSanity(raft.utils.id.genIdent()), 0);
  req.writeUInt32BE((Date.now() - raft.common.constants.REQUEST_UPDATE_IN_PAST_DELTA_MAX + 2000) / 1000 >>> 0, 0);
  t.strictEquals(checkRequestSanity(req), 0);
  req.writeUInt32BE((Date.now() - raft.common.constants.REQUEST_UPDATE_IN_PAST_DELTA_MAX) / 1000 >>> 0, 0);
  t.strictEquals(checkRequestSanity(req), -1);
  req.writeUInt32BE((Date.now() + raft.common.constants.REQUEST_UPDATE_IN_FUTURE_DELTA_MAX + 2000) / 1000 >>> 0, 0);
  t.strictEquals(checkRequestSanity(req), 1);
  t.end();
});

test('mixinReaders', t => {
  var o = {};
  mixinReaders(o);
  t.type(o.readTypeOf     , 'function');
  t.type(o.readTermOf     , 'function');
  t.type(o.readRequestIdOf, 'function');
  t.type(o.readDataOf     , 'function');
  t.strictEquals(o.readTypeOf     , readers.readTypeOf);
  t.strictEquals(o.readTermOf     , readers.readTermOf);
  t.strictEquals(o.readRequestIdOf, readers.readRequestIdOf);
  t.strictEquals(o.readDataOf     , readers.readDataOf);
  t.end();
});


test('LogEntry', t => {
  var buf = Buffer.from([17,18,19,20,21,22,23,24,25,26,27,28,2,0xff,0xff,0xff,0xff,0xff,0xff,0x1f,0xc0]);
  var entry = new LogEntry(buf, 42);
  t.type(entry, LogEntry);
  t.type(entry, Buffer);
  t.notStrictEquals(entry, buf);
  t.strictEquals(entry.buffer, buf.buffer);
  t.strictEquals(entry.byteOffset, buf.byteOffset);
  t.strictEquals(entry.byteLength, buf.byteLength);
  t.strictEquals(LogEntry.isLogEntry(entry), true);
  t.strictEquals(LogEntry.isLogEntry(buf), false);
  t.strictEquals(entry.isLogEntry, true);
  t.strictEquals(buf.isLogEntry, undefined);
  t.strictEquals(entry.entryType, 2);
  t.strictEquals(entry.isStateEntry, false);
  t.strictEquals(entry.isConfigEntry, false);
  t.strictEquals(entry.isCheckpointEntry, true);
  entry.entryType = 0;
  t.strictEquals(entry.entryType, 0);
  t.strictEquals(entry.isStateEntry, true);
  t.strictEquals(entry.isConfigEntry, false);
  t.strictEquals(entry.isCheckpointEntry, false);
  entry.entryType = 1;
  t.strictEquals(entry.entryType, 1);
  t.strictEquals(entry.isStateEntry, false);
  t.strictEquals(entry.isConfigEntry, true);
  t.strictEquals(entry.isCheckpointEntry, false);
  t.strictEquals(entry.hasRequestExpired, true);
  t.strictEquals(entry.checkRequestSanity, -1);
  t.type(entry.requestId, Buffer);
  t.strictEquals(entry.requestId.length, 12);
  t.deepEquals(entry.requestId, entry.readRequestId());
  t.deepEquals(entry.requestId, Buffer.from([17,18,19,20,21,22,23,24,25,26,27,28]));
  t.deepEquals(entry.readRequestId(), Buffer.from([17,18,19,20,21,22,23,24,25,26,27,28]));
  t.deepEquals(entry.readRequestKey(), '1112131415161718191a1b1c');
  t.strictEquals(entry.readEntryTerm(), Number.MAX_SAFE_INTEGER);
  t.strictEquals(entry.writeEntryTerm(271), undefined);
  t.strictEquals(entry.readEntryTerm(), 271);
  t.deepEquals(entry.readEntryData(), Buffer.from([0xc0]));

  t.test('bufferToLogEntry', t => {
    var buf = Buffer.from([17,18,19,20,21,22,23,24,25,26,27,28,2,0xff,0xff,0xff,0xff,0xff,0xff,0x1f,0xc0]);
    var entry = LogEntry.bufferToLogEntry(buf, 42);
    t.type(entry, LogEntry);
    t.type(entry, Buffer);
    t.strictEquals(entry, buf);
    t.strictEquals(LogEntry.isLogEntry(entry), true);
    t.strictEquals(LogEntry.isLogEntry(buf), true);
    t.strictEquals(entry.isLogEntry, true);
    t.strictEquals(buf.isLogEntry, true);
    t.end();
  });

  t.test('build', t => {
    var entry = LogEntry.build(null, 2, Number.MAX_SAFE_INTEGER, [0xde,0xad,0xba,0xca], 888);
    t.type(entry, LogEntry);
    t.type(entry, Buffer);
    t.strictEquals(LogEntry.isLogEntry(entry), true);
    t.strictEquals(entry.isLogEntry, true);
    t.strictEquals(entry.entryType, 2);
    t.strictEquals(entry.isStateEntry, false);
    t.strictEquals(entry.isConfigEntry, false);
    t.strictEquals(entry.isCheckpointEntry, true);
    t.strictEquals(entry.readEntryTerm(), Number.MAX_SAFE_INTEGER);
    t.deepEquals(entry.readRequestId(), Buffer.alloc(12));
    t.deepEquals(entry.readEntryData(), Buffer.from([0xde,0xad,0xba,0xca]));
    var ident = raft.utils.id.genIdent();
    var entry = LogEntry.build(ident, 2, Number.MAX_SAFE_INTEGER, [0xde,0xad,0xba,0xca], 888);
    t.type(entry, LogEntry);
    t.type(entry, Buffer);
    t.strictEquals(entry.readRequestKey(), ident);
    var ident = raft.utils.id.genIdent('buffer');
    var entry = LogEntry.build(ident, 2, Number.MAX_SAFE_INTEGER, [0xde,0xad,0xba,0xca], 888);
    t.type(entry, LogEntry);
    t.type(entry, Buffer);
    t.deepEquals(entry.readRequestId(), ident);
    t.end();
  })

  t.end();
});

test('UpdateRequest', t => {
  var buf = Buffer.from([0xc0]);
  var req = new UpdateRequest(buf, raft.utils.id.genIdent());
  t.type(req, UpdateRequest);
  t.type(req, Buffer);
  t.notStrictEquals(req, buf);
  t.strictEquals(req.buffer, buf.buffer);
  t.strictEquals(req.byteOffset, buf.byteOffset);
  t.strictEquals(req.byteLength, buf.byteLength);
  t.strictEquals(UpdateRequest.isUpdateRequest(req), true);
  t.strictEquals(UpdateRequest.isUpdateRequest(buf), false);
  t.type(req.requestId, 'string');

  req = new UpdateRequest(buf);
  t.type(req, UpdateRequest);
  t.type(req, Buffer);
  t.notStrictEquals(req, buf);
  t.strictEquals(req.buffer, buf.buffer);
  t.strictEquals(req.byteOffset, buf.byteOffset);
  t.strictEquals(req.byteLength, buf.byteLength);
  t.strictEquals(UpdateRequest.isUpdateRequest(req), false);
  t.strictEquals(UpdateRequest.isUpdateRequest(buf), false);
  t.type(req.requestId, 'undefined');

  t.test('bufferToUpdateRequest', t => {
    var buf = Buffer.from([0xc0]);
    var req = UpdateRequest.bufferToUpdateRequest(buf, raft.utils.id.genIdent('buffer'));
    t.type(req, UpdateRequest);
    t.type(req, Buffer);
    t.strictEquals(req, buf);
    t.strictEquals(UpdateRequest.isUpdateRequest(req), true);
    t.strictEquals(UpdateRequest.isUpdateRequest(buf), true);
    t.type(buf.requestId, Buffer);
    t.type(req.requestId, Buffer);
    t.end();
  });

  t.end();
});
