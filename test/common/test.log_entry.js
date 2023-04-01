/*
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const { LogEntry
      , UpdateRequest
      , makeHasRequestExpired
      , makeCheckRequestSanity
      , readers
      , mixinReaders
      , LOG_ENTRY_HEADER_SIZE } = raft.common.LogEntry;

test('should have functions and properties', t => {
  t.equal(LOG_ENTRY_HEADER_SIZE, 20);
  t.type(LogEntry               , 'function');
  t.type(UpdateRequest          , 'function');
  t.type(makeHasRequestExpired  , 'function');
  t.type(makeCheckRequestSanity , 'function');
  t.type(readers.readTypeOf     , 'function');
  t.type(readers.readTermOf     , 'function');
  t.type(readers.readRequestIdOf, 'function');
  t.type(readers.readDataOf     , 'function');
  t.type(mixinReaders           , 'function');
  t.end();
});

test('readers', t => {
  t.equal(readers.readTypeOf(Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,77,0,0,0,0,0,0,0])), 77);
  t.equal(readers.readTermOf(Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,0,7,6,5,4,3,2,1])), 7+6*0x100+5*0x10000+4*0x1000000+3*0x100000000+2*0x10000000000+0x1000000000000);
  t.same(readers.readRequestIdOf(Buffer.from([1,2,3,4,5,6,7,8,9,10,11,12,0,0,0,0,0,0,0,0])), Buffer.from([1,2,3,4,5,6,7,8,9,10,11,12]));
  t.same(readers.readRequestIdOf(Buffer.from([1,2,3,4,5,6,7,8,9,10,11,12,0,0,0,0,0,0,0,0]), 'hex'), '0102030405060708090a0b0c');
  t.same(readers.readDataOf(Buffer.alloc(20)), Buffer.alloc(0));
  t.same(readers.readDataOf(Buffer.alloc(21)), Buffer.from([0]));
  t.end();
});

test('makeHasRequestExpired', t => {
  const { DEFAULT_REQUEST_ID_TTL } = raft.common.constants;
  const hasRequestExpired = makeHasRequestExpired(DEFAULT_REQUEST_ID_TTL);
  var req = Buffer.alloc(12);
  t.equal(hasRequestExpired(req), true);
  t.equal(hasRequestExpired('000000000000000000000000'), true);
  t.equal(hasRequestExpired('ffffffffffffffffffffffff'), false);
  t.equal(hasRequestExpired(Buffer.from('000000000000000000000000ffffffffffffffffffffffff','hex'), 12), false);
  t.equal(hasRequestExpired(Buffer.from('ffffffffffffffffffffffff000000000000000000000000','hex'), 12), true);
  t.equal(hasRequestExpired(raft.utils.id.genIdent('buffer')), false);
  t.equal(hasRequestExpired(raft.utils.id.genIdent()), false);
  req.writeUInt32BE((Date.now() - DEFAULT_REQUEST_ID_TTL + 2000) / 1000 >>> 0, 0);
  t.equal(hasRequestExpired(req), false);
  req.writeUInt32BE((Date.now() - DEFAULT_REQUEST_ID_TTL) / 1000 >>> 0, 0);
  t.equal(hasRequestExpired(req), true);
  t.end();
});

test('makeCheckRequestSanity', t => {
  const { DEFAULT_REQUEST_ID_TTL, REQUEST_ID_TTL_ACCEPT_MARGIN } = raft.common.constants;
  const REQUEST_ID_IN_PAST_DELTA_MAX = DEFAULT_REQUEST_ID_TTL - REQUEST_ID_TTL_ACCEPT_MARGIN;
  const REQUEST_ID_IN_FUTURE_DELTA_MAX = REQUEST_ID_TTL_ACCEPT_MARGIN;
  const checkRequestSanity = makeCheckRequestSanity(DEFAULT_REQUEST_ID_TTL, REQUEST_ID_TTL_ACCEPT_MARGIN, REQUEST_ID_TTL_ACCEPT_MARGIN);
  var req = Buffer.alloc(12);
  t.equal(checkRequestSanity(req), -1);
  t.equal(checkRequestSanity('000000000000000000000000'), -1);
  t.equal(checkRequestSanity(raft.utils.id.genIdent('buffer')), 0);
  t.equal(checkRequestSanity(raft.utils.id.genIdent()), 0);
  req.writeUInt32BE((Date.now() - REQUEST_ID_IN_PAST_DELTA_MAX + 2000) / 1000 >>> 0, 0);
  t.equal(checkRequestSanity(req), 0);
  req.writeUInt32BE((Date.now() - REQUEST_ID_IN_PAST_DELTA_MAX) / 1000 >>> 0, 0);
  t.equal(checkRequestSanity(req), -1);
  req.writeUInt32BE((Date.now() + REQUEST_ID_IN_FUTURE_DELTA_MAX + 2000) / 1000 >>> 0, 0);
  t.equal(checkRequestSanity(req), 1);
  t.end();
});

test('mixinReaders', t => {
  var o = {};
  mixinReaders(o);
  t.type(o.readTypeOf     , 'function');
  t.type(o.readTermOf     , 'function');
  t.type(o.readRequestIdOf, 'function');
  t.type(o.readDataOf     , 'function');
  t.equal(o.readTypeOf     , readers.readTypeOf);
  t.equal(o.readTermOf     , readers.readTermOf);
  t.equal(o.readRequestIdOf, readers.readRequestIdOf);
  t.equal(o.readDataOf     , readers.readDataOf);
  t.end();
});


test('LogEntry', t => {
  var buf = Buffer.from([17,18,19,20,21,22,23,24,25,26,27,28,2,0xff,0xff,0xff,0xff,0xff,0xff,0x1f,0xc0]);
  var entry = new LogEntry(buf, 42);
  t.type(entry, LogEntry);
  t.type(entry, Buffer);
  t.not(entry, buf);
  t.equal(entry.buffer, buf.buffer);
  t.equal(entry.byteOffset, buf.byteOffset);
  t.equal(entry.byteLength, buf.byteLength);
  t.equal(LogEntry.isLogEntry(entry), true);
  t.equal(LogEntry.isLogEntry(buf), false);
  t.equal(entry.isLogEntry, true);
  t.equal(buf.isLogEntry, undefined);
  t.equal(entry.entryType, 2);
  t.equal(entry.isStateEntry, false);
  t.equal(entry.isConfigEntry, false);
  t.equal(entry.isCheckpointEntry, true);
  entry.entryType = 0;
  t.equal(entry.entryType, 0);
  t.equal(entry.isStateEntry, true);
  t.equal(entry.isConfigEntry, false);
  t.equal(entry.isCheckpointEntry, false);
  entry.entryType = 1;
  t.equal(entry.entryType, 1);
  t.equal(entry.isStateEntry, false);
  t.equal(entry.isConfigEntry, true);
  t.equal(entry.isCheckpointEntry, false);
  t.type(entry.requestId, Buffer);
  t.equal(entry.requestId.length, 12);
  t.same(entry.requestId, entry.readRequestId());
  t.same(entry.requestId, Buffer.from([17,18,19,20,21,22,23,24,25,26,27,28]));
  t.same(entry.readRequestId(), Buffer.from([17,18,19,20,21,22,23,24,25,26,27,28]));
  t.same(entry.readRequestKey(), '1112131415161718191a1b1c');
  t.equal(entry.readEntryTerm(), Number.MAX_SAFE_INTEGER);
  t.equal(entry.writeEntryTerm(271), undefined);
  t.equal(entry.readEntryTerm(), 271);
  t.equal(entry.writeEntryTerm(Number.MAX_SAFE_INTEGER), undefined);
  t.equal(entry.readEntryTerm(), Number.MAX_SAFE_INTEGER);
  t.same(entry.readEntryData(), Buffer.from([0xc0]));

  t.test('bufferToLogEntry', t => {
    var buf = Buffer.from([17,18,19,20,21,22,23,24,25,26,27,28,2,0xff,0xff,0xff,0xff,0xff,0xff,0x1f,0xc0]);
    var entry = LogEntry.bufferToLogEntry(buf, 42);
    t.type(entry, LogEntry);
    t.type(entry, Buffer);
    t.equal(entry, buf);
    t.equal(LogEntry.isLogEntry(entry), true);
    t.equal(LogEntry.isLogEntry(buf), true);
    t.equal(entry.isLogEntry, true);
    t.equal(buf.isLogEntry, true);
    t.end();
  });

  t.test('build', t => {
    var entry = LogEntry.build(null, 2, Number.MAX_SAFE_INTEGER, [0xde,0xad,0xba,0xca], 888);
    t.type(entry, LogEntry);
    t.type(entry, Buffer);
    t.equal(LogEntry.isLogEntry(entry), true);
    t.equal(entry.isLogEntry, true);
    t.equal(entry.entryType, 2);
    t.equal(entry.isStateEntry, false);
    t.equal(entry.isConfigEntry, false);
    t.equal(entry.isCheckpointEntry, true);
    t.equal(entry.readEntryTerm(), Number.MAX_SAFE_INTEGER);
    t.same(entry.readRequestId(), Buffer.alloc(12));
    t.same(entry.readEntryData(), Buffer.from([0xde,0xad,0xba,0xca]));
    var ident = raft.utils.id.genIdent();
    var entry = LogEntry.build(ident, 2, Number.MAX_SAFE_INTEGER, [0xde,0xad,0xba,0xca], 888);
    t.type(entry, LogEntry);
    t.type(entry, Buffer);
    t.equal(entry.readRequestKey(), ident);
    var ident = raft.utils.id.genIdent('buffer');
    var entry = LogEntry.build(ident, 2, Number.MAX_SAFE_INTEGER, [0xde,0xad,0xba,0xca], 888);
    t.type(entry, LogEntry);
    t.type(entry, Buffer);
    t.same(entry.readRequestId(), ident);
    t.end();
  })

  t.end();
});

test('UpdateRequest', t => {
  var buf = Buffer.from([0xc0]);
  var req = new UpdateRequest(buf, raft.utils.id.genIdent());
  t.type(req, UpdateRequest);
  t.type(req, Buffer);
  t.not(req, buf);
  t.equal(req.buffer, buf.buffer);
  t.equal(req.byteOffset, buf.byteOffset);
  t.equal(req.byteLength, buf.byteLength);
  t.equal(UpdateRequest.isUpdateRequest(req), true);
  t.equal(UpdateRequest.isUpdateRequest(buf), false);
  t.type(req.requestId, 'string');

  req = new UpdateRequest(buf);
  t.type(req, UpdateRequest);
  t.type(req, Buffer);
  t.not(req, buf);
  t.equal(req.buffer, buf.buffer);
  t.equal(req.byteOffset, buf.byteOffset);
  t.equal(req.byteLength, buf.byteLength);
  t.equal(UpdateRequest.isUpdateRequest(req), false);
  t.equal(UpdateRequest.isUpdateRequest(buf), false);
  t.type(req.requestId, 'undefined');

  t.test('bufferToUpdateRequest', t => {
    var buf = Buffer.from([0xc0]);
    var req = UpdateRequest.bufferToUpdateRequest(buf, raft.utils.id.genIdent('buffer'));
    t.type(req, UpdateRequest);
    t.type(req, Buffer);
    t.equal(req, buf);
    t.equal(UpdateRequest.isUpdateRequest(req), true);
    t.equal(UpdateRequest.isUpdateRequest(buf), true);
    t.type(buf.requestId, Buffer);
    t.type(req.requestId, Buffer);
    t.end();
  });

  t.end();
});
