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
  var chunk = new SnapshotChunk(buf, 1, 2, 3);
  t.type(chunk, SnapshotChunk);
  t.type(chunk, Buffer);
  t.notStrictEquals(chunk, buf);
  t.strictEquals(chunk.buffer, buf.buffer);
  t.strictEquals(chunk.byteOffset, buf.byteOffset);
  t.strictEquals(chunk.byteLength, buf.byteLength);
  t.strictEquals(chunk.isFirstChunk, false);
  t.strictEquals(chunk.isLastChunk, true);
  t.strictEquals(SnapshotChunk.isSnapshotChunk(chunk), true);
  t.strictEquals(SnapshotChunk.isSnapshotChunk(buf), false);
  t.strictEquals(chunk.isSnapshotChunk, true);
  t.strictEquals(buf.isSnapshotChunk, undefined);
  t.strictEquals(chunk.logIndex, 1);
  t.strictEquals(chunk.snapshotByteOffset, 2);
  t.strictEquals(chunk.snapshotTotalLength, 3);

  chunk = new SnapshotChunk(buf, 1, 0, 1);
  t.notStrictEquals(chunk, buf);
  t.strictEquals(chunk.isFirstChunk, true);
  t.strictEquals(chunk.isLastChunk, true);

  chunk = new SnapshotChunk(buf, 1, 2, 4);
  t.notStrictEquals(chunk, buf);
  t.strictEquals(chunk.isFirstChunk, false);
  t.strictEquals(chunk.isLastChunk, false);

  t.test('bufferToSnapshotChunk', t => {
    var buf = Buffer.from([0xc0]);
    var chunk = SnapshotChunk.bufferToSnapshotChunk(buf, 1, 0, 3);
    t.type(chunk, SnapshotChunk);
    t.type(chunk, Buffer);
    t.strictEquals(chunk, buf);
    t.strictEquals(chunk.isFirstChunk, true);
    t.strictEquals(chunk.isLastChunk, false);
    t.strictEquals(SnapshotChunk.isSnapshotChunk(chunk), true);
    t.strictEquals(SnapshotChunk.isSnapshotChunk(buf), true);
    t.strictEquals(chunk.isSnapshotChunk, true);
    t.strictEquals(buf.isSnapshotChunk, true);
    t.strictEquals(chunk.logIndex, 1);
    t.strictEquals(chunk.snapshotByteOffset, 0);
    t.strictEquals(chunk.snapshotTotalLength, 3);
    t.strictEquals(buf.logIndex, 1);
    t.strictEquals(buf.snapshotByteOffset, 0);
    t.strictEquals(buf.snapshotTotalLength, 3);
    t.end();
  });

  t.end();
});
