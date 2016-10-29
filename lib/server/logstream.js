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

      return this.fileLog._indexFileOf(index).then(indexFile => {
        if (this._logStream === false) {
          console.log('already canceled');
          return;
        }
        var input = []
          , lastIndex = min(indexFile.nextIndex - 1, this.lastIndex)
          , extractor = indexFile.createEntryExtractor(input, index, lastIndex)
          , highWaterMark = max(INDEX_FILE_STREAM_HWMARK_MIN, this.maxChunkSize)
          , logStream = this._logStream = indexFile.createLogReadStream(index, lastIndex, highWaterMark)
          , unref = () => {
            if (indexFile) {
              --indexFile._cacheRefs;
              // console.warn("UNREF: %s",indexFile._cacheRefs)
              indexFile = null;
            }
          };
        /* ref indexFile avoiding cache purging */
        ++indexFile._cacheRefs;
        logStream.on('cancel', unref);
        logStream.on('error', err => {
          unref();
          this.emit('error', err);
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
            this.emit('error', err);
          }
        })
        .on('end', () => {
          unref();
          if (!extractor.next().done || input.length !== 0) {
            this.emit('error', new Error("log read stream ended prematurely"))
          } else if (this._logStream !== false) {
            this._index = lastIndex + 1;
            this._logStream = null;
            this._read();
          }
        });
      }).catch(err => this.emit('error', err));
    }
  }
}

module.exports = exports = LogStream;
