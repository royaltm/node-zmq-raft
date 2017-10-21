/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const { Writable } = require('stream');

const synchronize = require('../utils/synchronize');

class LogWriter extends Writable {
  constructor(log) {
    super({objectMode: true});
    this.log = log;
    this.snapshot = null;
    this.nextSnapshotOffset = 0;
  }

  _write(chunk, encoding, callback) {
    const log = this.log;
    synchronize(log, () => {
      if (chunk.isLogEntry) {
        return log._writeEntryUncommitted(chunk, chunk.logIndex);
      }
      else if (!chunk.isSnapshotChunk) {
        throw new Error("invalid chunk");
      }
    }).then(() => {
      if (chunk.isSnapshotChunk) {
        let promise
          , snapshot = this.snapshot;
        if (chunk.isFirstChunk) {
          snapshot && snapshot.ready().then(s => s.close());
          snapshot = this.snapshot = log.createTmpSnapshot(chunk.logIndex, chunk.logTerm, chunk.snapshotTotalLength);
          this.nextSnapshotOffset = chunk.length;
          promise = snapshot.ready().then(s => s.write(chunk, 0, chunk.length));
        }
        else if (this.nextSnapshotOffset !== chunk.snapshotByteOffset || !snapshot) {
          throw new Error("missing snapshot chunk");
        }
        else {
          this.nextSnapshotOffset += chunk.length;
          promise = snapshot.write(chunk, chunk.snapshotByteOffset, chunk.length);
        }
        return promise.then(() => {
          if (chunk.isSnapshotChunk && chunk.isLastChunk) {
            let snapshot = this.snapshot;
            this.snapshot = null;
            return log.installSnapshot(snapshot);
          }
        });
      }
    })
    .then(() => callback(), callback);
  }

  commit() {
    return synchronize(this.log, () => this.log._commitLastIndexFile());
  }
}

module.exports = LogWriter;

