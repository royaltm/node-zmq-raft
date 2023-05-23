/*
 *  Copyright (c) 2016-2023 Rafał Michalski <royal@yeondir.com>
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
  var chunk = new SnapshotChunk(buf, 1, 2, 3, 4);
  t.type(chunk, SnapshotChunk);
  t.type(chunk, Buffer);
  t.not(chunk, buf);
  t.equal(chunk.buffer, buf.buffer);
  t.equal(chunk.byteOffset, buf.byteOffset);
  t.equal(chunk.byteLength, buf.byteLength);
  t.equal(chunk.isFirstChunk, false);
  t.equal(chunk.isLastChunk, true);
  t.equal(SnapshotChunk.isSnapshotChunk(chunk), true);
  t.equal(SnapshotChunk.isSnapshotChunk(buf), false);
  t.equal(chunk.isSnapshotChunk, true);
  t.equal(buf.isSnapshotChunk, undefined);
  t.equal(chunk.logIndex, 1);
  t.equal(chunk.snapshotByteOffset, 2);
  t.equal(chunk.snapshotTotalLength, 3);
  t.equal(chunk.logTerm, 4);

  chunk = new SnapshotChunk(buf, 1, 0, 1);
  t.not(chunk, buf);
  t.equal(chunk.isFirstChunk, true);
  t.equal(chunk.isLastChunk, true);

  chunk = new SnapshotChunk(buf, 1, 2, 4);
  t.not(chunk, buf);
  t.equal(chunk.isFirstChunk, false);
  t.equal(chunk.isLastChunk, false);

  t.test('bufferToSnapshotChunk', t => {
    var buf = Buffer.from([0xc0]);
    var chunk = SnapshotChunk.bufferToSnapshotChunk(buf, 1, 0, 3, 1001);
    t.type(chunk, SnapshotChunk);
    t.type(chunk, Buffer);
    t.equal(chunk, buf);
    t.equal(chunk.isFirstChunk, true);
    t.equal(chunk.isLastChunk, false);
    t.equal(SnapshotChunk.isSnapshotChunk(chunk), true);
    t.equal(SnapshotChunk.isSnapshotChunk(buf), true);
    t.equal(chunk.isSnapshotChunk, true);
    t.equal(buf.isSnapshotChunk, true);
    t.equal(chunk.logIndex, 1);
    t.equal(chunk.snapshotByteOffset, 0);
    t.equal(chunk.snapshotTotalLength, 3);
    t.equal(chunk.logTerm, 1001);
    t.equal(buf.logIndex, 1);
    t.equal(buf.snapshotByteOffset, 0);
    t.equal(buf.snapshotTotalLength, 3);
    t.equal(buf.logTerm, 1001);
    t.end();
  });

  t.end();
});
