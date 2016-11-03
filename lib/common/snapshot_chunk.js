/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";
const setPrototypeOf = Object.setPrototypeOf
    , isBuffer = Buffer.isBuffer

const { defineConst } = require('../utils/helpers');

/* in node 4 and later Buffer hackishly descends from Uint8Array */
class SnapshotChunk extends Buffer {
  /* Creates a new SnapshotChunk instance from a buffer without copying its bytes */
  constructor(buffer, logIndex, snapshotByteOffset, snapshotTotalLength) {
    if (!isBuffer(buffer)) {
      throw new TypeError("SnapshotChunk: provided buffer argument is not a buffer");
    }
    var chunk = setPrototypeOf(new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength), SnapshotChunk.prototype);
    chunk.logIndex = logIndex;
    chunk.snapshotByteOffset = snapshotByteOffset;
    chunk.snapshotTotalLength = snapshotTotalLength;
    return chunk;
  }

  get isFirstChunk() {
    return this.snapshotByteOffset === 0;
  }

  get isLastChunk() {
    return this.snapshotByteOffset + this.length === this.snapshotTotalLength;
  }

  static isSnapshotChunk(chunk) {
    return chunk instanceof SnapshotChunk;
  }

  /* Converts buffer instance to SnapshotChunk (replaces its __proto__) */
  static bufferToSnapshotChunk(buffer, logIndex, snapshotByteOffset, snapshotTotalLength) {
    if (!isBuffer(buffer)) {
      throw new TypeError("SnapshotChunk: provided buffer argument is not a buffer");
    }
    var chunk = setPrototypeOf(buffer, SnapshotChunk.prototype);
    chunk.logIndex = logIndex;
    chunk.snapshotByteOffset = snapshotByteOffset;
    chunk.snapshotTotalLength = snapshotTotalLength;
    return chunk;
  }
}

defineConst(SnapshotChunk.prototype, 'isSnapshotChunk', true);

defineConst(SnapshotChunk, 'SnapshotChunk', SnapshotChunk);

module.exports = exports = SnapshotChunk;
