/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const min = Math.min
    , max = Math.max;

const { Readable } = require('stream');

const INDEX_FILE_STREAM_HWMARK_MIN = 8*1024;
const DEFAULT_MAX_CHUNK_ENTRIES = 4096;
const DEFAULT_MAX_CHUNK_SIZE = 128*1024;

const { defineConst } = require('../utils/helpers');

class LogStream extends Readable {
  constructor(fileLog, firstIndex, lastIndex, options) {
    options = options ? Object.assign({}, options) : {};
    options.objectMode = true;

    super(options);

    this.maxChunkEntries = (options.maxChunkEntries >>> 0) || DEFAULT_MAX_CHUNK_ENTRIES;
    this.maxChunkSize = (options.maxChunkSize >>> 0) || DEFAULT_MAX_CHUNK_SIZE;

    this._index = firstIndex;
    this._logStream = null;

    defineConst(this, 'fileLog', fileLog);
    defineConst(this, 'firstIndex', firstIndex);
    defineConst(this, 'lastIndex', lastIndex);

    this.once('cancel', () => {
      var logStream = this._logStream;
      if (logStream && 'object' === typeof logStream) {
        logStream.pause();
        logStream.emit('cancel');
      }
      this._logStream = false;
      this.pause();
    });
  }

  cancel() {
    this.emit('cancel');
  }

  _read(/* size */) {
    var logStream = this._logStream;

    if (logStream && 'object' === typeof logStream) {

      logStream.resume();

    } else if (logStream === null) {
      this._logStream = true;

      var index = this._index;
      if (index > this.lastIndex) return this.push(null);

      this.fileLog._indexFileOf(index, indexFile => new Promise((resolve, reject) => {
        if (this._logStream === false) {
          /* already canceled */
          return resolve(); /* unlock indexFile */
        }
        var input = []
          , lastIndex = min(indexFile.nextIndex - 1, this.lastIndex)
          , extractor = indexFile.createEntryExtractor(input, index, lastIndex)
          , highWaterMark = max(INDEX_FILE_STREAM_HWMARK_MIN, this.maxChunkSize)
          , logStream = this._logStream = indexFile.createLogReadStream(index, lastIndex, highWaterMark);

        logStream
        .on('cancel', resolve) /* unlock indexFile */
        .on('error', reject)
        .on('end', () => {
          if (!extractor.next().done || input.length !== 0) {
            reject(new Error("log read stream ended prematurely")); /* unlock indexFile */
          }
          else {
            resolve(); /* unlock indexFile */
            if (this._logStream !== false) {
              this._index = lastIndex + 1;
              this._logStream = null;
              this._read();
            }
          }
        })
        .on('data', chunk => {
          var maxChunkEntries = this.maxChunkEntries
            , maxChunkSize = this.maxChunkSize
            , entries, size, item, buf;

          try {

            input.push(chunk);

            do {
              entries = [];
              size = 0;

              do {
                item = extractor.next();
                buf = item.value;
                if (buf === undefined) break;
                size += buf.length;
                entries.push(buf);
              } while (entries.length < maxChunkEntries && size < maxChunkSize);

              if (entries.length !== 0 && !this.push(entries)) {
                logStream.pause();
              }

            } while (buf !== undefined);

            // if (item.done) console.warn('it is done!: %s', input.length)
            if (item.done && input.length !== 0) {
              // console.warn(input.map(b=>b.length))
              throw new Error("too much log stream data");
            }

          } catch(err) {
            logStream.pause();
            reject(err); /* unlock indexFile */
          }
        });
      })).catch(err => this.emit('error', err));
    }
  }
}

module.exports = exports = LogStream;
