/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray;

const assert = require('assert');
const os = require('os');
const fs = require('fs');
const { createWriteStream, createReadStream } = fs;
const path = require('path');

const debug = require('debug')('zmq-raft:snapshotfile');

const mp = require('msgpack-lite');

const { open, openDir, close, closeDir, fdatasync, ftruncate, fstat,
        fsyncDirFileCloseDir, link, mkdirp, read, renameSyncDir, write} = require('../utils/fsutil');

const { defineConst } = require('../utils/helpers');

const { TOKEN_HEADER_SIZE
      , TOKEN_HEADER_BYTE_SIZE
      , BYTES_PER_ELEMENT
      , tokenToUint32, findToken, createTokenFile } = require('../utils/tokenfile');

const { mixin: mixinHistoryRotation, createRotateName } = require('../utils/filerotate');

const ReadyEmitter = require('../common/readyemitter');

const VERSION = 1;
const HEADER_SIZE = 9;
const HEADER_BODY_SIZE = HEADER_SIZE - TOKEN_HEADER_SIZE;
const SNAP = tokenToUint32('SNAP');
const DATA = tokenToUint32('DATA');
const META = tokenToUint32('META');

const padBuf = Buffer.alloc(BYTES_PER_ELEMENT);

const emptyBuf = new Buffer(0);

const fd$         = Symbol.for('fd');
const dataOffset$ = Symbol.for('dataOffset');
const setReady$   = Symbol.for('setReady');
const filename$   = Symbol.for('filename');

/*

Index File Format (numbers in little endian order)

The file consists of segements. First segment is the SNAP optionally followed by META or more segments followed by DATA marker follwed by snapshot data.

The SNAP segment:

offs. content

  0 | "SNAP"
  4 | 4-byte LSB header size: 36
  8 | 01 00 00 00 - version
 12 | 8-byte LSB the log index of the snapshot entry
 20 | 8-byte LSB the term of the snapshot entry
 28 | 8-byte LSB data size: the size of the snapshot data

The META segment:

  0 | "META"
  4 | 4-byte LSB size of the segment
  8 | msgpacked map: {created: "iso date string", hostname: hostname, software: some software info....}
    | 0 to 3 padding bytes, number of padding bytes are calculated as: 4 - (size & 3)

Any other segment:

  0 | 4-byte segment tag
  4 | 4-byte LSB size of the segment
  8 | data
    | 0 to 3 padding bytes, number of padding bytes are calculated as: 4 - (size & 3)

The marker:

  0 | "DATA"
  4 | 0x00000000
  8 | the beginning of the data


var snap = new SnapshotFile('filename'[, index, term, datasize]);
var snap = SnapshotFile('filename')

snap.logIndex
snap.logTerm
snap.dataSize

snap.read(position, size[, buffer, offset])
snap.write(buffer, position, length, offset)
snap.sync()
snap.replace('destname')
snap.dataReadStream()
snap.close()

*/

class SnapshotFile extends ReadyEmitter {
  /**
   * @param {string} filename
   * @param {uint} [index]
   * @param {uint} [term]
   * @param {uint|stream.Reader} [dataSize]
   * @return {SnapshotFile}
  **/
  constructor(filename, index, term, dataSize) {
    super();

    if (!filename || 'string' !== typeof filename) throw new TypeError("SnapshotFile: filename must be a non-empty string");
    this[filename$] = filename;

    const ready = (fd, dataOffset, logIndex, logTerm, dataSize) => {
      this[fd$] = fd;
      this[dataOffset$] = dataOffset;
      defineConst(this, 'logIndex', logIndex);
      defineConst(this, 'logTerm',  logTerm);
      defineConst(this, 'dataSize', dataSize);
      debug('ready "%s"', filename);
      this[setReady$]();
    };

    if (arguments.length > 1) {
      /* will create new snapshot file */
      if (arguments.length < 4) throw new Error("SnapshotFile: filename, index, term, dataSize required to create snapshot file");
      if (!Number.isFinite(index) || index % 1 !== 0 || index < 0 || index > Number.MAX_SAFE_INTEGER) throw new TypeError("index should be a positive integer");
      if (!Number.isFinite(term) || term % 1 !== 0 || term < 0 || term > Number.MAX_SAFE_INTEGER) throw new TypeError("term should be a positive integer");
      if (!Number.isFinite(dataSize) || dataSize % 1 !== 0 || dataSize < 0 || dataSize > Number.MAX_SAFE_INTEGER) {
        if ('object' !== typeof dataSize || !dataSize) {
          throw new TypeError("dataSize should be a positive integer or a stream.Reader");
        }
        debug('creating "%s" logIndex: %s, logTerm: %s from stream', filename, index, term);
      }
      else debug('creating "%s" logIndex: %s, logTerm: %s, dataSize: %s', filename, index, term, dataSize);

      createSnapshotFile(filename, index, term, dataSize)
      .then(([fd, dataOffset, dataSize]) => ready(fd, dataOffset, index, term, dataSize))
      .catch(err => this.error(err));
    }
    else {
      debug('reading "%s"', filename);
      open(filename, 'r+').then(fd => readSnapshotFile(fd)
        .then(([dataOffset, index, term, dataSize]) => {
          debug('read "%s" logIndex: %s, logTerm: %s, dataSize: %s', filename, index, term, dataSize);
          ready(fd, dataOffset, index, term, dataSize);
        })
      ).catch(err => this.error(err));
    }
  }

  toString() {
    return this[filename$];
  }

  get filename() {
    return this[filename$];
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
    else Promise.resolve();
  }

  /**
   * true if file is closed
   *
   * @property {number}
  **/
  get isClosed() {
    return this[fd$] === undefined;
  }

  /**
   * read snapshot data
   *
   * resolves to buffer or slice of the buffer with read data
   *
   * @param {number} position - position in snapshot
   * @param {number} length - bytes to read
   * @param {Buffer} [buffer] - buffer to write data to
   * @param {number} [offset] - offset in the buffer to start writing at
   * @return {Promise}
  **/
  read(position, length, buffer, offset) {
    offset >>>= 0;
    length >>>= 0;
    position = +position;

    if (buffer === undefined) {
      offset = 0;
      buffer = Buffer.allocUnsafe(length);
    }
    if (!Buffer.isBuffer(buffer)) Promise.reject(new TypeError("SnapshotFile.read: buffer must be a Buffer instance"));
    if (position < 0 || position + length > this.dataSize) Promise.reject(new Error("SnapshotFile.read: position or length out of bounds"));
    if (length === 0) return Promise.resolve(emptyBuf);
    if (offset + length > buffer.length) return Promise.reject(new Error("SnapshotFile.read: offset or length exceed buffer capacity"));
    return read(this[fd$], buffer, offset, length, position + this[dataOffset$])
           .then(() => (offset === 0 && length === buffer.length ? buffer
                                                                 : buffer.slice(offset, offset + length)));
  }

  /**
   * write snapshot data
   *
   * resolves to bytes written
   *
   * index must be <= nextIndex and >= firstAvailableIndex
   * if index is less than nextIndex the log is first being
   * rolled back to the entry before the index
   *
   * @param {Buffer} buffer - buffer to write data from
   * @param {number} position - position in snapshot
   * @param {number} length - bytes to write
   * @param {number} [offset] - offset in the buffer to start writing at
   * @return {Promise}
  **/
  write(buffer, position, length, offset) {
    offset >>>= 0;
    length >>>= 0;
    position = +position;

    if (!Buffer.isBuffer(buffer)) Promise.reject(new TypeError("SnapshotFile.write: buffer must be a Buffer instance"));
    if (position < 0 || position + length > this.dataSize) Promise.reject(new Error("SnapshotFile.write: position or length out of bounds"));
    if (length === 0) return Promise.resolve(0);
    if (offset + length > buffer.length) return Promise.reject(new Error("SnapshotFile.write: offset or length exceed buffer capacity"));
    return write(this[fd$], buffer, offset, length, position + this[dataOffset$])
  }

  /**
   * ensures snapshot data is durable
   *
   * @return {Promise}
  **/
  sync() {
    return fdatasync(this[fd$]);
  }

  /**
   * atomically replaces destination file with current snapshot file creating backup if needed
   *
   * @param {string} destname
   * @return {Promise}
  **/
  replace(destname) {
    if (!destname || 'string' !== typeof destname) throw new TypeError("SnapshotFile.replace: destname must be a non-empty string");

    var filename = this.filename;
    if (destname === filename) throw new Error("SnapshotFile.replace: destname is not different from filename");

    return this.sync()
    .then(() => link(destname, createRotateName(destname))
      .catch(err => {
        if (err.code !== 'ENOENT') throw err;
      })
    ).then(() => renameSyncDir(filename, destname)
    ).then(() => {
      this.triggerHistoryRotation();
      debug('file "%s" replaced "%s"', filename, destname);
      return this[filename$] = destname;
    });
  }

  /**
   * create data read stream
   *
   * @param {number} [position]
   * @return {ReadStream}
  **/
  createDataReadStream(position) {
    position || (position = 0);
    position = parseInt(position);
    if (isNaN(position) || position < 0 || position > this.dataSize) throw new Error("SnapshotFile.dataReadStream: position out of bounds");
    return createReadStream(null, {fd: this[fd$], autoClose: false, start: position + this[dataOffset$]});
  }

}

defineConst(SnapshotFile, 'VERSION', VERSION);

mixinHistoryRotation(SnapshotFile.prototype, debug);

module.exports = exports = SnapshotFile;

/* utils */

function createSnapshotFile(filename, index, term, dataSize, reader) {
  if ('object' === typeof dataSize && 'function' === typeof dataSize.pipe) {
    reader = dataSize, dataSize = 0;
  }
  return openDir(path.dirname(filename)).then(dirfd => {
    if (reader) dataSize = 0;
    return createTokenFile(filename).then(tokenFile => {
      const fd = tokenFile.fd
          , headerByteSize = HEADER_BODY_SIZE * BYTES_PER_ELEMENT
          , headerBuf = Buffer.allocUnsafe(headerByteSize)
          , header = new Uint32Array(headerBuf.buffer, headerBuf.byteOffset, HEADER_BODY_SIZE)
      header[0] = VERSION;
      header[1] = index    >>> 0; header[2] = index    / 0x100000000 >>> 0;
      header[3] = term     >>> 0; header[4] = term     / 0x100000000 >>> 0;
      header[5] = dataSize >>> 0; header[6] = dataSize / 0x100000000 >>> 0;

      const meta = mp.encode({created: new Date().toJSON(), hostname: os.hostname()});

      tokenFile.appendToken(SNAP, headerByteSize, headerBuf);
      tokenFile.appendToken(META, meta.length, meta);
      return tokenFile.appendToken(DATA, 0)
      .then(dataOffset => {
        if (reader) {
          const writer = createWriteStream(null, {fd: fd, autoClose: false, start: dataOffset});
          const promise = new Promise((resolve, reject) => {
            writer.on('finish', resolve).on('error', reject);
          });
          reader.on('data', chunk => dataSize += chunk.length);
          reader.pipe(writer);
          return promise.then(() => {
            header[0] = dataSize >>> 0;
            header[1] = dataSize / 0x100000000 >>> 0;
            return write(fd, headerBuf, 0, 2 * BYTES_PER_ELEMENT, 5 * BYTES_PER_ELEMENT + TOKEN_HEADER_BYTE_SIZE)
            .then(() => dataOffset);
          });
        }
        else return dataOffset;
      })
      .then(dataOffset => fsyncDirFileCloseDir(dirfd, fd).then(() => [fd, dataOffset, dataSize]))
      .catch(err => {
        fs.close(fd);
        throw err;
      });
    }).catch(err => {
      closeDir(dirfd);
      throw err;
    });
  }).catch(err => {
    if (err.code !== 'ENOENT') throw(err);
    return mkdirp(path.dirname(filename)).then(() => createSnapshotFile(filename, index, term, dataSize, reader));
  });
}

function readSnapshotFile(fd) {
  const headerByteSize = HEADER_SIZE * BYTES_PER_ELEMENT;
  const headerBuf = Buffer.allocUnsafe(headerByteSize);
  const header = new Uint32Array(headerBuf.buffer, headerBuf.byteOffset, HEADER_SIZE);
  return read(fd, headerBuf, 0, headerByteSize, 0).then(() => {
    if (header[0] !== SNAP
        || header[1] !== headerByteSize - TOKEN_HEADER_BYTE_SIZE
        || header[2] !== VERSION) {
      throw new Error("readSnapshotFile: snapshot file type mismatch");
    }

    var index    = header[3] + header[4] * 0x100000000;
    var term     = header[5] + header[6] * 0x100000000;
    var dataSize = header[7] + header[8] * 0x100000000;

    return findToken(fd, DATA, headerByteSize)
    .then(([dataOffset]) => fstat(fd).then(stat => {
      if (stat.size - dataOffset !== dataSize) throw new Error("readSnapshotFile: invalid snapshot file");
      return [dataOffset, index, term, dataSize];      
    }));
  });
}

/*

var SnapshotFile = require('./lib/common/snapshotfile');
var size = 1024*1024*1024,total = 10*size
var snap = new SnapshotFile('./tmp/snap.tmp',1,0,total);
var snap = new SnapshotFile('./tmp/snap');
snap.ready().then(n=>console.log(n), console.error)
async function writeAll() {
  await snap.ready();
  var i = 0;
  for(var i = 0; i < total; i+=size) {
    let data = crypto.randomBytes(size);
    await snap.write(data, i, data.length);
    console.log(i);
  }
  await snap.replace('./tmp/snap');
  console.log('replaced');
}
writeAll().then(n=>console.log(n), console.error)

var snap2 = new SnapshotFile('./tmp/snap.tmp',1,0,snap.createDataReadStream(0));
snap2.ready().then(n=>console.log(n), console.error)

snap.logIndex
snap.logTerm
snap.dataSize
snap.write(crypto.randomBytes(100), 0, 100).then(n=>console.log(n), console.error)
snap.replace('snap').then(n=>console.log(n), console.error)

snap.close()
var snap = new SnapshotFile('snap').on('ready', ()=>console.log('ok')).on('error', console.error);
var s=snap.createDataReadStream(0)

*/
