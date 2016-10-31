/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray
    , isBuffer = Buffer.isBuffer;

const assert = require('assert');
const os = require('os');
const fs = require('fs');
const { createWriteStream, createReadStream } = fs;
const path = require('path');
const EventEmitter = require('events');

const mp = require('msgpack-lite');

const { open, openDir, close, closeDir, read, write, ftruncate, fdatasync,
        fsyncDirFileCloseDir, mkdirp, unlink} = require('../utils/fsutil');

const { defineConst } = require('../utils/helpers');
const { BYTES_PER_ELEMENT, tokenToUint32, findToken } = require('../utils/tokenfile');

const ReadyEmitter = require('../common/readyemitter');

const INDEX_FILE_EXT = '.rlog';

const emptyBuf = Buffer.alloc(0);
const padBuf = Buffer.alloc(BYTES_PER_ELEMENT);

const VERSION = 1;
const HEADER_SIZE = 6;
const INDEX_OFFSET = HEADER_SIZE * BYTES_PER_ELEMENT;
const DEFAULT_CAPACITY = 0x4000;
const MAX_CAPACITY = 0x00ffffff;
const MAX_DATASIZE = Math.min(require('buffer').kMaxLength, 0x3fffffff);
const RLOG = tokenToUint32('RLOG');
const ITMZ = tokenToUint32('ITMZ');
const META = tokenToUint32('META');

const debug = require('debug')('raft-indexfile');

const fd$ = Symbol.for('fd');
const nextAt$ = Symbol.for('nextAt');
const setReady$ = Symbol.for('setReady');

/*

Index File Format (numbers in little endian order)

The file consists of segements. First segment is the RLOG optionally followed by META or more segments followed by ITMZ marker follwed by log entries data.

The RLOG segment:

offs. content

  0 | "RLOG"
  4 | 4-byte LSB header size: 16 + capacity * 4
  8 | 01 00 00 00 - version
 12 | 4-byte LSB capacity: how many entries this file can contain, must be 0 < capacity <= MAX_CAPACITY
 16 | 8-byte LSB index of the first entry
 24 | 4-byte LSB relative offset after the first entry counted from beginning of data (offset after index + 0 entry)
 28 | 4-byte LSB relative offset after the second entry counted from beginning of data (offset after index + 1 entry)
...
    | 4-byte LSB relative offset after the last entry counted from beginning of data (offset after index + capacity entry)

The META segment:

  0 | "META"
  4 | 4-byte LSB size of msgpacked data
  8 | msgpacked map: {created: "iso date string", hostname: hostname, software: some software info....}
    | 0 to 3 padding bytes, number of padding bytes are calculated as: 4 - (size & 3)

Any other segment:

  0 | 4-byte segment tag
  4 | 4-byte LSB size of the data
  8 | data
    | 0 to 3 padding bytes, number of padding bytes are calculated as: 4 - (size & 3)

The marker:

  0 | "ITMZ"
  4 | 0x00000000
  8 | the beginning of the log entries

There are some limits imposed on the entries:

- Each entry's size must be > 0 bytes.
- Total number of bytes of all entries must be less than MAX_DATASIZE

The number of entries written so far is determined by the number of non-zero offsets in the index table.
The first offset == 0 indicates the next available index.

The log entry offsets are always written to 4 byte aligned index table, so no single offset will reside on the boundary of the file sector.
This way as long as disk file system doesn't allow partially written sectors to be part of the updated file there is no way the log file will be corrupted other than by disk/file system error.

On appending, the entry data is being written first, then the offset pointing after the entry is written at index table, thus commiting write
On rollback only offset table is being zeroed - starting from the index being rolled back to.
There's an additional safety step before commiting offset of the new entry that makes sure the offset of the next entry (if available) is 0.
If it's not 0 the offset table will be cleared first starting from the first "dirty" entry until clear entry is found or end of the table.

*/

class IndexFile extends ReadyEmitter {
  /**
   * @param {string} dir|filename - root directory or path to index file
   * @param {number} [index] - first entry index only if first argument is directory
   * @param {number} [capacity] - capacity of the new file created, ignored if file exists
   * @return {IndexFile}
  **/
  constructor(dir, index, capacity) {
    super();
    if (!dir || 'string' !== typeof dir) throw new TypeError("IndexFile: first argument must be a directory or file name");

    var openExisting = false;
    if (arguments.length === 1) {
      openExisting = true;
      debug('opening existing: "%s"', dir);
      [dir, index] = getDirIndexFromLogPath(dir);
    }

    capacity  = (capacity || DEFAULT_CAPACITY) >>> 0;
    if (capacity < 1 && capacity > MAX_CAPACITY) throw new TypeError("IndexFile: capacity should be a number less than MAX_CAPACITY");
    if ('number' !== typeof index || isNaN(index) || index < 0 || index % 1 !== 0 || index > (Number.MAX_SAFE_INTEGER - capacity)) throw new TypeError("index should be a positive integer");
    capacity = capacity - index % capacity;

    defineConst(this, 'index', index);
    const basename = defineConst(this, 'basename', logBaseName(index));
    const filename = defineConst(this, 'filename', logPath(dir, basename));

    const ready = (fd, offsets, buffer, position, capacity, nextAt) => {
      defineConst(this, 'position', position);
      defineConst(this, 'buffer', buffer);
      defineConst(this, 'offsets', offsets);
      defineConst(this, 'capacity', capacity);
      this[fd$] = fd;
      this[nextAt$] = nextAt;
      this[setReady$]();
    }

    open(filename, 'r+').then(fd => readIndexFile(fd, index)
                                    .then(([[offsets, buffer, position, capacity], nextAt]) => {
        debug('opened: %s', basename);
        ready(fd, offsets, buffer, position, capacity, nextAt);
      })
    , err => {
      if (openExisting || err.code !== "ENOENT") throw err;
      debug('creating: %s with capacity: %s', basename, capacity);
      return createIndexFile(filename, index, capacity).then(([fd, position]) => {
        const offsets = new Uint32Array(capacity);
        const buffer = Buffer.from(offsets.buffer, offsets.byteOffset, offsets.byteLength);
        ready(fd, offsets, buffer, position, capacity, 0);
      });
    }).catch(err => this.error(err));
  }

  toString() {
    return this.basename;
  }

  /**
   * close file
   *
   * @return {Promise}
  **/
  close() {
    var fd = this[fd$];
    if (fd !== undefined) {
      delete this[fd$];
      return close(fd);
    }
    else return Promise.resolve();
  }

  /**
   * true if file is closed
   *
   * @property {number}
  **/
  get isClosed() {
    return this.isReady && this[fd$] === undefined;
  }

  /**
   * clear log, close and delete the file
   *
   * @return {Promise}
  **/
  destroy() {
    return this.truncate(this.index).then(() => this.close()).then(() => unlink(this.filename));
  }

  /**
   * return true if index is present in this file
   *
   * @param {number} index
   * @return {boolean}
  **/
  includes(index) {
    index -= this.index;
    return index >= 0 && index < this[nextAt$];
  }

  /**
   * return true if this file is capable of storing index number
   *
   * @param {number} index
   * @return {boolean}
  **/
  allowed(index) {
    index -= this.index;
    return index >= 0 && index < this.capacity;
  }

  /**
   * get byte offset within the file of the existing index entry or the next entry
   *
   * @param {number} index
   * @return {number|undefined}
  **/
  getByteOffset(index) {
    index -= this.index;
    if (index >= 0 && index <= this[nextAt$]) {
      return this.position + (index > 0 ? this.offsets[index - 1] : 0);
    }
  }

  /**
   * get size in bytes of `count` entries starting at `index`
   *
   * @param {number} index
   * @param {number} count
   * @return {number|undefined}
  **/
  getByteSize(index, count) {
    var offsets = this.offsets;
    index -= this.index;
    count >>>= 0;
    if (index >= 0 && index + count <= this[nextAt$]) {
      if (count === 0) return 0;
      return offsets[index + count - 1] - (index > 0 ? offsets[index - 1] : 0);
    }
  }

  /**
   * estimate how many log entries would fit into given number of bytes
   *
   * @param {number} index
   * @param {number} count
   * @param {number} maxlength
   * @return {number|undefined}
  **/
  countEntriesFitSize(index, count, maxlength) {
    maxlength >>= 0;
    var size = this.getByteSize(index, count);
    if (isNaN(size)) throw new Error("IndexFile: index out of bounds");
    if (size > maxlength) {
      var lo = 1, hi = count - 1;
      count = (lo + hi + 1) >> 1;
      size = this.getByteSize(index, count);
      while(lo < hi) {
        if (size > maxlength) hi = count - 1; else lo = count;
        count = (lo + hi + 1) >> 1;
        size = this.getByteSize(index, count);
      }
      if (size > maxlength) return 0;
    }
    return count;
  }

  /**
   * next index number
   *
   * @property {number}
  **/
  get nextIndex() {
    return this.index + this[nextAt$];
  }

  /**
   * number of free index slots
   *
   * @return {number}
  **/
  get free() {
    return this.capacity - this[nextAt$];
  }

  /**
   * first allowed index
   *
   * @property {number}
  **/
  get firstAllowedIndex() {
    return this.index;
  }

  /**
   * last allowed index
   *
   * @property {number}
  **/
  get lastAllowedIndex() {
    return this.index + this.capacity - 1;
  }

  /**
   * read log data
   *
   * resolves to a buffer
   *
   * @param {number} index - first entry index
   * @param {number} count - number of entries
   * @return {Promise}
  **/
  read(index, count) {
    var position = this.getByteOffset(index);
    var length = this.getByteSize(index, count);
    if (isNaN(length)) return Promise.reject(new Error("IndexFile: index out of bounds"));
    if (length === 0) return Promise.resolve(emptyBuf);
    const buffer = Buffer.allocUnsafe(length);
    return read(this[fd$], buffer, 0, length, position).then(() => buffer);
  }

  /**
   * read log data entries
   *
   * resolves to an array of buffers
   *
   * @param {number} index - first entry index
   * @param {number} count - number of entries
   * @return {Promise}
  **/
  readv(index, count) {
    return this.read(index, count).then(buffer => this.splitb(index, count, buffer));
  }

  /**
   * read log data into provided buffer
   *
   * resolves to length of data written in bytes or negative length if data would not fit in the buffer
   *
   * @param {number} index - first entry index
   * @param {number} count - number of entries
   * @param {Buffer} buffer - buffer to write data to
   * @param {number} [offset] - offset in the buffer to start writing at
   * @return {Promise}
  **/
  readb(index, count, buffer, offset) {
    offset >>>= 0;
    var position = this.getByteOffset(index);
    var length = this.getByteSize(index, count);
    if (isNaN(length)) return Promise.reject(new Error("IndexFile: index out of bounds"));
    if (length === 0) return Promise.resolve(0);
    if (offset + length > buffer.length) return Promise.resolve(-length);
    return read(this[fd$], buffer, offset, length, position);
  }

  /**
   * read single log's entry data slice into provided buffer
   *
   * resolves to the length of the data read in bytes or negative length if data would not fit in the buffer
   *
   * @param {number} index - entry index
   * @param {number} start - position within log data to start reading from, negative position counts from the end
   * @param {number} stop - position within log data to stop reading at, negative position counts from the end
   * @param {Buffer} buffer - buffer to write data to
   * @param {number} [offset] - offset in the buffer to start writing at
   * @return {Promise}
  **/
  readSlice(index, start, stop, buffer, offset) {
    var entryLength = this.getByteSize(index, 1);
    if (isNaN(entryLength)) return Promise.reject(new Error("IndexFile: index out of bounds"));
    start >>= 0;
    stop >>= 0;
    offset >>>= 0;
    if (start < 0) start += entryLength;
    if (stop < 0) stop += entryLength;
    if (start < 0 || stop > entryLength) return Promise.reject(new Error("position not within log entry's data"));
    var length = stop - start;
    if (length <= 0) return Promise.resolve(0);
    if (offset + length > buffer.length) return Promise.resolve(-length);
    return read(this[fd$], buffer, offset, length, start + this.getByteOffset(index));
  }

  /**
   * split log data in the provided buffer using log entry offsets
   *
   * returns array of buffer slices
   *
   * @param {number} index - first entry index
   * @param {number} count - number of entries
   * @param {Buffer} buffer - buffer to split
   * @param {number} [offset] - offset in the buffer to start splitting at
   * @return {Array}
  **/
  splitb(index, count, buffer, offset) {
    offset >>>= 0;
    count >>>= 0;
    index -= this.index;
    var stop = index + count;
    if (isNaN(stop) || index < 0 || stop > this[nextAt$]) throw new Error("IndexFile: index out of bounds");
    var offsets = this.offsets;
    var position = (index > 0 ? offsets[index - 1] : 0);
    var length = buffer.length;
    var result = [];

    while(index < stop) {
      var size = offsets[index++];
      var end = offset + size - position;
      if (end > length) break;
      position = size;
      result.push(buffer.slice(offset, end));
      offset = end;
    }

    return result;
  }

  /**
   * create log data entries buffer extractor using log entry offsets
   *
   * returns an iterator that will create entries from buffers provided in the input array
   *
   * the iterator will yield Buffer instances as an entry data or undefined if
   * couldn't compose entire entry from provided buffers
   * before yielding undefined value the iterator will shift already processed
   * buffers from input array
   * it's possible to push more buffers into the input array between iterations
   * no other input array modification is allowed (except pushing new buffers)
   * the iterator will finish only when the lastIndex entry has been reached
   *
   * @param {Array} input - array of buffers to extract entries from
   * @param {number} firstIndex - first expected entry index
   * @param {number} lastIndex - last expected entry index
   * @return {Iterator}
  **/
  createEntryExtractor(input, firstIndex, lastIndex) {
    firstIndex -= this.index;
    lastIndex -= this.index;
    var endIndex = this[nextAt$]
    if (isNaN(firstIndex) || firstIndex < 0 || firstIndex >= endIndex) throw new Error("IndexFile: firstIndex out of bounds");
    if (isNaN(lastIndex) || lastIndex < 0 || lastIndex >= endIndex) throw new Error("IndexFile: lastIndex out of bounds");

    return bufferEntryExtractor(input, this.offsets, firstIndex, lastIndex + 1);
  }

  /**
   * create log data read stream
   *
   * @param {number} firstIndex
   * @param {number} lastIndex
   * @param {number|Object} options (highWaterMark if a number)
   * @return {ReadStream}
  **/
  createLogReadStream(firstIndex, lastIndex, options) {
    var posStart = this.getByteOffset(firstIndex);
    var posEnd = this.getByteOffset(lastIndex + 1) - 1;
    if (isNaN(posStart)) throw new Error("IndexFile.createLogReadStream: firstIndex out of bounds");
    if (isNaN(posEnd)) throw new Error("IndexFile.createLogReadStream: lastIndex out of bounds");

    var opts = {fd: this[fd$], autoClose: false, start: posStart, end: posEnd};
    if (options !== undefined) {
      if ('number' === typeof options) opts.highWaterMark = options;
      else if (options !== null && 'object' === typeof options) {
        opts = Object.assign({}, options, opts);
      }
      else throw new TypeError("IndexFile.createLogReadStream: options should be a number or an object");
    }

    return createReadStream(null, opts);
  }

  /**
   * truncate log beginning at given index
   *
   * entries before index are retained
   *
   * index must be <= nextIndex and >= firstAllowedIndex
   *
   * @param {number} index
   * @return {Promise}
  **/
  truncate(index) {
    var nextAt = this[nextAt$];
    if ('number' === typeof index) index -= this.index; else index = nextAt;
    if (index < 0 || index > this.capacity) return Promise.reject(new Error("IndexFile.clear: index out of bounds"));

    if (index === nextAt) { /* nothing happens */
      return Promise.resolve();
    }
    else if (index < nextAt) { /* truncate log */
      this[nextAt$] = index;
      return truncate(this[fd$], this.buffer, index, nextAt - index);
    }
    else {
      return Promise.reject(new Error("IndexFile.clear: index after nextIndex"));
    }
  }

  /**
   * append log data
   *
   * resolves to [numUnwrittenEntries, indexOfNextEntry]
   *
   * @param {Buffer|Array<Buffer>|ArrayArray<Buffer>} data
   * @return {Promise}
  **/
  append(data) {
    if (isArray(data)) {
      return this.writev(data);
    }
    else return this.write(data);
  }

  /**
   * write single log entry at index
   *
   * resolves to [numUnwrittenEntries, indexOfNextEntry]
   *
   * given index must be <= nextIndex and >= firstAllowedIndex
   * if index is less than nextIndex the log is first being
   * rolled back to the entry before the index
   *
   * if index not given appends data to the end of the log
   *
   * @param {Buffer} data
   * @param {number} [index]
   * @return {Promise}
  **/
  write(data, index) {
    var nextAt = this[nextAt$];
    if ('number' === typeof index) index -= this.index; else index = nextAt;
    if (index < 0 || index > this.capacity) return Promise.reject(new Error("IndexFile: index out of bounds"));

    const dataoffset = this.position;
    const offsets = this.offsets;
    const buffer = this.buffer;
    const fd = this[fd$];
    const currentsize = nextAt > 0 ? offsets[nextAt - 1] : 0;

    const entrysize = data.length;
    if (entrysize <= 0) return Promise.reject(new Error("entry size must be > 0"));

    const writeAt = (position) => {
      var nextpos = position + entrysize;
      if (index === this.capacity || nextpos > MAX_DATASIZE) return Promise.resolve([1, this.index + index]);

      return write(fd, data, 0, entrysize, dataoffset + position)
              /* cleanup index, might be dirty after unfinished rollback write  */
        .then(() => clearAndWriteIfDirty(fd, buffer, offsets, index + 1))
        .then(() => {
          offsets[index] = nextpos;
          return commitwrite(fd, buffer, index, 1, offsets);
        })
        .then(() => [0, this.index + (this[nextAt$] = index + 1)]);
    };

    if (index === nextAt) { /* add new entry */
      return writeAt(currentsize);
    }
    else if (index < nextAt) { /* truncate log and add new entry */
      this[nextAt$] = index;
      return truncate(fd, buffer, index, nextAt - index).then(
        () => writeAt(index > 0 ? offsets[index - 1] : 0));
    }
    else {
      return Promise.reject(new Error("non linear addition"));
    }
  }

  /**
   * write many log data entries at index
   *
   * resolves to [numUnwrittenEntries, indexOfNextEntry]
   *
   * given `index` must be <= nextIndex and >= firstAllowedIndex
   * if index is less than nextIndex the log is first being
   * rolled back to the entry before the index
   *
   * if `index` is not given appends data to the end of the log
   *
   * if `datav` given as an array of arrays of buffers then buffers in the
   * each inner array are being written as the single log entry
   *
   * @param {Array<Buffer>|Array<Array<Buffer>>} datav
   * @param {number} [index]
   * @return {Promise}
  **/
  writev(datav, index) {
    var nextAt = this[nextAt$];
    if ('number' === typeof index) index -= this.index; else index = nextAt;
    if (index < 0 || index > this.capacity) return Promise.reject(new Error("IndexFile: index out of bounds"));

    const dataoffset = this.position;
    const offsets = this.offsets;
    const buffer = this.buffer;
    const fd = this[fd$];
    const currentsize = nextAt > 0 ? offsets[nextAt - 1] : 0;
    const numitems = datav.length;

    if (index + numitems > this.capacity) {
      datav = datav.slice(0, this.capacity - index);
    }

    const writeAt = (position) => writeBuffersUpTo(
                                fd, datav, offsets, index, dataoffset, position, MAX_DATASIZE - position)
      .then(numwritten => {
        if (numwritten !== 0) {
          /* cleanup index, might be dirty after unfinished rollback write  */
          return clearAndWriteIfDirty(fd, buffer, offsets, index + numwritten)
          .then(() => commitwrite(fd, buffer, index, numwritten, offsets))
          .then(
            () => [numitems - numwritten, this.index + (this[nextAt$] = index + numwritten)]
          );
        }
        else return [numitems, this.index + index];
     });

    if (index === nextAt) { /* add new entries */
      return writeAt(currentsize);
    }
    else if (index < nextAt) { /* truncate log and add new entries */
      this[nextAt$] = index;
      return truncate(fd, buffer, index, nextAt - index).then(
        () => writeAt(index > 0 ? offsets[index - 1] : 0));
    }
    else {
      return Promise.reject(new Error("non linear addition"));
    }
  }

}

IndexFile.logPath = logPath;
IndexFile.logPathComponents = logPathComponents;
IndexFile.logBaseName = logBaseName;
IndexFile.INDEX_FILE_EXT = INDEX_FILE_EXT;
IndexFile.INDEX_FILENAME_LENGTH = (Number.MAX_SAFE_INTEGER.toString(16) + INDEX_FILE_EXT).length;
defineConst(IndexFile, 'VERSION', VERSION);
defineConst(IndexFile, 'DEFAULT_CAPACITY', DEFAULT_CAPACITY);
defineConst(IndexFile, 'MAX_CAPACITY', MAX_CAPACITY);
defineConst(IndexFile, 'MAX_DATASIZE', MAX_DATASIZE);

module.exports = IndexFile;

/* utils */

const logpad = "0".repeat(Number.MAX_SAFE_INTEGER.toString(16).length);
const INDEX_PATH_PREFIX_LENGTH = 9;
defineConst(IndexFile, 'INDEX_PATH_PREFIX_LENGTH', INDEX_PATH_PREFIX_LENGTH);

/* 1 => "00000000000001" */
function logBaseName(index) {
  index = 'number' === typeof index ? index.toString(16) : index;
  return logpad.length <= index.length ? index : logpad.substr(0, logpad.length - index.length) + index;
}

/* (dir, 1) => [dir, "00000", "00", "00", "00000000000001.rlog"] */
function logPathComponents(dir, index) {
  index = logBaseName(index);
  return [dir, index.slice(0,5), index.slice(5,7), index.slice(7,9), index + INDEX_FILE_EXT];
}

/* "dir/1234567890abcd" => "dir/12345/67/89/1234567890abcd" */
function logPath(dir, index) {
  return path.join(...logPathComponents(dir, index));
}

function getDirIndexFromLogPath(filepath) {
  var components = path.resolve(filepath).split(path.sep);
  var idx = components.length - 4;
  if (idx > 0) {
    var match = components[idx + 3].match(/^(([0-f]{5})([0-f]{2})([0-f]{2})[0-f]{5})\.rlog$/);
    if (match && match[2] === components[idx]
              && match[3] === components[idx + 1]
              && match[4] === components[idx + 2]) {
      return [filepath.slice(0, -path.join.apply(path, components.slice(idx)).length-1), parseInt(match[1], 16)];
    }
  }
  throw new TypeError("IndexFile: invalid log filename: " + filepath);
}

/**
 * clear non-zero offsets starting at index
 *
 * @param {Uint32Array} offsets
 * @param {number} index
 * @return {number} number of items cleared
**/
function clear(offsets, index) {
  const length = offsets.length;
  for(var i = index; i < length; ++i) {
    if (offsets[i] === 0) break;
    offsets[i] = 0;
  }
  return i - index;
}

/**
 * write buffers to file indicated by desciptor
 *
 * resolves to numwritten
 *
 * @param {number} fd - destination file descriptor
 * @param {Array<Buffer>|Array<Array<Buffer>>} buffers - array of entries to write
 * @param {Uint32Array} offsets - offset array to write positions to
 * @param {number} indexStart - index at which start writing positions to offsets
 * @param {number} dataoffset - offset to add to position of entries written
 * @param {number} limit - maximum number of bytes that are allowed to write
 * @return {Promise}
**/
function writeBuffersUpTo(fd, buffers, offsets, indexStart, dataoffset, position, limit) {
  const length = buffers.length;
  if (length === 0 || getEntrySize(buffers[0]) > limit) return Promise.resolve(0);

  return new Promise((resolve, reject) => {
    const writer = createWriteStream(null, {fd: fd, autoClose: false, start: dataoffset + position});
    writer.on('error', err => {
      writer.removeListener('drain', write);
      reject(err);
    });

    var size = 0, index = 0;

    const finish = () => {
      writer.removeListener('drain', write);
      writer.end(() => resolve(index));
    };

    const write = () => {
      var data, entrysize;
      do {
        if (index >= length) {
          finish();
          break;
        }
        data = buffers[index];
        entrysize = getEntrySize(data);
        if (entrysize <= 0) {
          writer.removeListener('drain', write);
          writer.end();
          reject(new Error("entry size must be > 0"));
          break;
        }
        if ((size += entrysize) > limit) {
          finish();
          break;
        }
        offsets[indexStart + index] = position + size;
        ++index;
      } while (writeEntry(writer, data));
    };
    writer.on('drain', write);
    write();
  });
}

function writeEntry(writer, data) {
  if (isArray(data)) {
    var ret, i = 0, len = data.length;
    while (i < len) ret = writer.write(data[i++]);
    return ret;
  }
  else return writer.write(data);
}

function getEntrySize(data) {
  if (isArray(data)) {
    var size = 0, i = data.length;
    while (i-- > 0) size += data[i].length;
    return size;
  }
  else return data.length;
}

/**
 * commit log offsets to a file
 *
 * @param {number} fd - destination file descriptor
 * @param {Buffer} buffer - data representing offsets
 * @param {number} index - index of first offset to write
 * @param {number} numwritten - number of offsets to write
 * @param {Uint32Array} offsets - offset array, may be cleared on error
**/
function commitwrite(fd, buffer, index, numwritten, offsets) {
  var ioffs = index * BYTES_PER_ELEMENT;
  return write(fd, buffer, ioffs, numwritten * BYTES_PER_ELEMENT, INDEX_OFFSET + ioffs)
          .then(() => fdatasync(fd))
          .catch(err => {
            clear(offsets, index);
            throw err;
          });
}

/**
 * Check if offset at index is clear otherwise clear offsets at index
 * and any non-clear offsets that follow and commit them to a file.
 *
 * @param {number} fd - destination file descriptor
 * @param {Buffer} buffer - data representing offsets
 * @param {Uint32Array} offsets - offset array
 * @param {number} index - index of first offset to check
**/
function clearAndWriteIfDirty(fd, buffer, offsets, index) {
  var ioffs, ilen = clear(offsets, index) * BYTES_PER_ELEMENT;
  if (ilen !== 0) {
    ioffs = index  * BYTES_PER_ELEMENT;
    return write(fd, buffer, ioffs, ilen, INDEX_OFFSET + ioffs).then(() => fdatasync(fd));
  }
  else return Promise.resolve();
}

/**
 * Clear offsets beginning at index and commit them to a file.
 *
 * @param {number} fd - destination file descriptor
 * @param {Buffer} buffer - data representing offsets
 * @param {number} index - index of first offset to clear
 * @param {number} numoffsets - number of items to clear
**/
function truncate(fd, buffer, index, numoffsets) {
  var ioffs = index * BYTES_PER_ELEMENT;
  var ilen = numoffsets * BYTES_PER_ELEMENT;
  buffer.fill(0, ioffs, ioffs + ilen);
  return write(fd, buffer, ioffs, ilen, INDEX_OFFSET + ioffs).then(() => fdatasync(fd));
}

function createIndexFile(filename, index, capacity) {
  return openDir(path.dirname(filename)).then(dirfd => {
    return open(filename, 'wx+').then(fd => {
      const headerBuf = Buffer.allocUnsafe(HEADER_SIZE * BYTES_PER_ELEMENT);
      const header = new Uint32Array(headerBuf.buffer, headerBuf.byteOffset, HEADER_SIZE);
      const indexSize = header.byteLength + capacity * BYTES_PER_ELEMENT;
      header[0] = RLOG;
      header[1] = indexSize - 2 * BYTES_PER_ELEMENT;
      header[2] = VERSION;
      header[3] = capacity;
      header[4] = index >>> 0;
      header[5] = index / 0x100000000 >>> 0;

      return write(fd, headerBuf, 0, headerBuf.length, 0).then(() => ftruncate(fd, indexSize))
      .then(() => {
        const meta = mp.encode({created: new Date().toJSON(), hostname: os.hostname()});
        header[0] = META;
        header[1] = meta.length;
        const padding = -meta.length & 3;
        header[2] = ITMZ;
        header[3] = 0;
        return new Promise((resolve, reject) => {
          const writer = createWriteStream(null, {fd: fd, autoClose: false, start: indexSize});
          writer.on('error', reject);
          writer.write(headerBuf.slice(0, 2 * BYTES_PER_ELEMENT));
          writer.write(meta);
          if (padding) writer.write(padBuf.slice(0, padding));
          writer.end(headerBuf.slice(2 * BYTES_PER_ELEMENT, 4 * BYTES_PER_ELEMENT), () => {
            fsyncDirFileCloseDir(dirfd, fd)
            .then(() => resolve([fd, indexSize + 2 * BYTES_PER_ELEMENT + meta.length + padding + 2 * BYTES_PER_ELEMENT])
                  , reject);
          });
        });
      }).catch(err => {
        fs.close(fd);
        throw err;
      });
    }).catch(err => {
      closeDir(dirfd);
      throw err;
    });
  }).catch(err => {
    if (err.code !== 'ENOENT') throw(err);
    return mkdirp(path.dirname(filename)).then(() => createIndexFile(filename, index, capacity));
  });
}

function readIndexFile(fd, index) {
  const headerBuf = Buffer.allocUnsafe(HEADER_SIZE * BYTES_PER_ELEMENT);
  const header = new Uint32Array(headerBuf.buffer, headerBuf.byteOffset, HEADER_SIZE);
  return read(fd, headerBuf, 0, header.byteLength, 0).then(bytesRead => {
    if (header[0] !== RLOG
        || header[1] !== header.byteLength + (header[3] - 2) * BYTES_PER_ELEMENT
        || header[2] !== VERSION
        || header[4] !== (index >>> 0)
        || header[5] !== (index / 0x100000000 >>> 0)) {
      throw new Error("readIndexFile: index file type mismatch");
    }

    var capacity = header[3];
    const indexSize = header.byteLength + capacity * BYTES_PER_ELEMENT;

    const buffer = Buffer.allocUnsafeSlow(capacity * BYTES_PER_ELEMENT);
    const offsets = new Uint32Array(buffer.buffer);

    return read(fd, buffer, 0, buffer.length, header.byteLength)
    .then(bytesRead => {
      var promise = findToken(fd, ITMZ, indexSize).then(([position]) => [offsets, buffer, position, capacity]);
      var nextAt, lastOffset = 0, len = offsets.length;

      for(nextAt = 0; nextAt < len; ++nextAt) {
        var offset = offsets[nextAt];
        if (offset === 0) break;
        if (offset <= lastOffset) {
          throw new Error("readIndexFile: sanity index check failed");
        }
        lastOffset = offset;
      }
      return Promise.all([promise, nextAt]);
    });
  });
}

function* bufferEntryExtractor(list, offsets, index, endIndex) {
  if (index >= endIndex) return;

  var position = (index > 0 ? offsets[index - 1] : 0)
    , size = offsets[index]
    , end = size - position;

  position = size;

  var bufsLength = 0;
  var start = 0;
  var offset = 0;
  var buffer, i = start;

  /* iterate until all entries has been extracted */
  for (;;) {
    /* scan buffers */
    scan:
    for (; i < list.length; ++i) {
      buffer = list[i];
      if (!isBuffer(buffer)) throw new TypeError('bufferEntryExtractor: list must be an Array of Buffers');
      bufsLength += buffer.length;

      /* is entry within currently scanned buffers */
      while (end <= bufsLength) {
        /* whole entry fits in single buffer */
        if (i === start) {
          /* extract slice of the buffer, next entry will continue in the same buffer */
          yield buffer.slice(offset, end);
        }
        /* entry spans across multiple buffers (at least 2) */
        else {
          /* create buffer large enough to fit the entire entry */
          let length = end - offset
            , entry = Buffer.allocUnsafe(length)
            /* entry might end somewhere within the current buffer */
            , sourceEnd = buffer.length + end - bufsLength
            , pos = length - sourceEnd
            , j = i, buf = buffer;

          /* end marker will point within the current buffer, do it here before we loose sourceEnd */
          end = sourceEnd;
          /* copy (backwards) all the buffers from current to start */
          do {
            buf.copy(entry, pos, 0, sourceEnd);
            buf = list[--j];
            sourceEnd = buf.length;
            pos -= sourceEnd;
          } while (j > start);
          /* entry will begin at offset in the buffer from the start */
          buf.copy(entry, 0, offset, sourceEnd);

          yield entry;

          /* next entry will begin within the current buffer */
          bufsLength = buffer.length;
          start = i;
        }

        /* last entry at the buffer boundary */
        if (end === bufsLength) {
          /* next entry starts from the next buffer */
          start = i + 1;
          end = bufsLength = 0;
        }

        /* that was the last possible entry */
        if (++index === endIndex) {
          break scan;
        }
        else {
          /* next entry's (offset, end) */
          offset = end;
          size = offsets[index];
          end = size - position + offset;
          position = size;
        }

      } /* process entry */

    } /* scan */

    /* no more buffers to scan, loose reference to the last buffer quickly */
    buffer = undefined;

    /* remove already processed buffers from input */
    if (start > 0) {
      if (start === 1) {
        list.shift();
      }
      else list.splice(0, start);

      i -= start;
      start = 0;
    }

    if (index === endIndex) break;

    /* wait for more to come */
    yield;
  }
}

/*
var IndexFile = require('./raft/indexfile');
var idx = new IndexFile('data/log/00000/00/00/00000000000001.rlog').on('error', err=>console.log(err.stack))

var idx = new IndexFile('data', 10000, 65536).on('error', err=>console.log(err.stack))
function add(index) {
  if (index > idx.lastAllowedIndex) {console.log('koniec'); return;}
  idx.write(crypto.randomBytes((Math.random()*5000+1)>>>0), index).then(([undone,index])=>{if (index % 100 === 0) console.log(index); if (!undone) add(index);}, err=>console.log(err));
}

function addm(index, count) {
  if (index > idx.lastAllowedIndex) {console.log('koniec'); return;}
  buffers = [];
  for(var i = 0; i < count; ++i) buffers.push(crypto.randomBytes((Math.random()*5000+1)>>>0));
  idx.writev(buffers, index).then(([undone,index])=>{console.log(index); if (!undone) addm(index, count);}, err=>console.log(err));
}
idx.read(0, 1).then(arg => console.log(arg), err => console.log(err))
idx.close()
for(var i = 0; i <= idx.capacity; ++i) assert(idx.countEntriesFitSize(idx.firstAllowedIndex,i,4391)===1)

*/
