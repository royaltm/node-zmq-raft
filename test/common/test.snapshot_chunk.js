/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const { SnapshotChunk
      , isSnapshotChunk } = raft.common.SnapshotChunk;

test('should have functions and properties', t => {
  t.type(SnapshotChunk  , 'function');
  t.type(isSnapshotChunk, 'function');
  t.end();
});

test('SnapshotChunk', t => {
  var buf = Buffer.from([0xc0]);
  var req = new SnapshotChunk(buf, 1, 2, 3);
  t.type(req, SnapshotChunk);
  t.type(req, Buffer);
  t.notStrictEquals(req, buf);
  t.strictEquals(req.buffer, buf.buffer);
  t.strictEquals(req.byteOffset, buf.byteOffset);
  t.strictEquals(req.byteLength, buf.byteLength);
  t.strictEquals(SnapshotChunk.isSnapshotChunk(req), true);
  t.strictEquals(SnapshotChunk.isSnapshotChunk(buf), false);
  t.strictEquals(req.isSnapshotChunk, true);
  t.strictEquals(buf.isSnapshotChunk, undefined);
  t.strictEquals(req.logIndex, 1);
  t.strictEquals(req.snapshotByteOffset, 2);
  t.strictEquals(req.snapshotTotalLength, 3);

  t.test('bufferToSnapshotChunk', t => {
    var buf = Buffer.from([0xc0]);
    var req = SnapshotChunk.bufferToSnapshotChunk(buf, 1, 2, 3);
    t.type(req, SnapshotChunk);
    t.type(req, Buffer);
    t.strictEquals(req, buf);
    t.strictEquals(SnapshotChunk.isSnapshotChunk(req), true);
    t.strictEquals(SnapshotChunk.isSnapshotChunk(buf), true);
    t.strictEquals(req.isSnapshotChunk, true);
    t.strictEquals(buf.isSnapshotChunk, true);
    t.strictEquals(req.logIndex, 1);
    t.strictEquals(req.snapshotByteOffset, 2);
    t.strictEquals(req.snapshotTotalLength, 3);
    t.strictEquals(buf.logIndex, 1);
    t.strictEquals(buf.snapshotByteOffset, 2);
    t.strictEquals(buf.snapshotTotalLength, 3);
    t.end();
  });

  t.end();
});
