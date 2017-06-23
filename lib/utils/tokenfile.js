/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const assert = require('assert');
const { read } = require('fs');
const { open, write, ftruncate } = require('../utils/fsutil');

const BYTES_PER_ELEMENT = Uint32Array.BYTES_PER_ELEMENT;

const TOKEN_HEADER_SIZE = 2;
const TOKEN_HEADER_BYTE_SIZE = TOKEN_HEADER_SIZE * BYTES_PER_ELEMENT;

exports.TOKEN_HEADER_SIZE      = TOKEN_HEADER_SIZE;
exports.TOKEN_HEADER_BYTE_SIZE = TOKEN_HEADER_BYTE_SIZE;
exports.BYTES_PER_ELEMENT      = BYTES_PER_ELEMENT;
exports.tokenToUint32          = tokenToUint32;
exports.pad4                   = pad4;
exports.findToken              = findToken;
exports.createTokenFile        = createTokenFile;

/**
 * Ensures the bytes is a multiplication of 4, increasing it if necessary
 *
 * @param {number} bytes
 * @return {number}
**/
function pad4(bytes) {
  return bytes + (-bytes & 3);
}

const tokenBuffer = Buffer.allocUnsafe(BYTES_PER_ELEMENT);
const tokenArray = new Uint32Array(tokenBuffer.buffer, tokenBuffer.byteOffset, 1);

/**
 * Convert token string to Uint32
 *
 * @param {string} token
 * @return {Uint32}
**/
function tokenToUint32(token) {
  assert.equal(Buffer.byteLength(token), BYTES_PER_ELEMENT);
  tokenBuffer.write(token);
  return tokenArray[0];
}

/**
 * Find next token data in a token file
 *
 * Resolves to [data_segment_offset, data_segment_size]
 *
 * @param {number} fd - file descriptor
 * @param {UInt32} token - unsigned 32-bit LSB integer token
 * @param {UInt32} position - position in a file to start searching from
 * @return {Promise}
**/
function findToken(fd, token, position) {
  const headerBuf = Buffer.allocUnsafe(2 * BYTES_PER_ELEMENT);
  const header = new Uint32Array(headerBuf.buffer, headerBuf.byteOffset, 2);
  return new Promise((resolve, reject) => {
    const seek = () => {
      read(fd, headerBuf, 0, headerBuf.length, position, (err, bytesRead) => {
        if (err) return reject(err);
        if (bytesRead !== headerBuf.length) return reject(new Error("tokenFile: bytesRead mismatch"));

        if (header[0] === token) return resolve([position + headerBuf.length, header[1]]);
        position += headerBuf.length + pad4(header[1]);
        seek();
      });
    };
    seek();
  });
}

function createTokenFile(filename) {
  return open(filename, 'wx+').then(fd => {
    return new TokenFile(fd, 0);
  });
}

class TokenFile {
  constructor(fd, position) {
    this.fd = fd;
    this.promise = Promise.resolve(position);
  }

  ensureBuffer(size) {
    var buffer = this.buffer;
    size += TOKEN_HEADER_BYTE_SIZE;
    if (buffer === undefined || buffer.length < size) {
      buffer = this.buffer = Buffer.allocUnsafe(pad4(size));
      this.header = new Uint32Array(buffer.buffer, buffer.byteOffset, TOKEN_HEADER_SIZE);
    }
    return buffer;
  }

  ready() {
    return this.promise;
  }

  appendToken(token, totalLength, inputBuffer, inputStart, inputLength) {
    return this.promise = this.promise.then(position => {
      const fd = this.fd
          , inputBufLength = inputBuffer && inputBuffer.length || 0;

      totalLength = parseInt(totalLength);
      if (!isFinite(totalLength)) totalLength = 0;

      inputStart >>>= 0;
      inputLength = (inputLength == null) ? inputBufLength
                                          : (inputLength >>> 0);

      if (inputStart + inputLength > inputBufLength) {
        inputLength = inputBufLength - inputStart;
        if (inputLength < 0) inputLength = 0;
      }
      if (inputLength > totalLength) inputLength = totalLength;

      if ('string' === typeof token) token = tokenToUint32(token);

      const buf = this.ensureBuffer(inputLength)
          , header = this.header;

      header[0] = token;
      header[1] = totalLength;

      if (inputLength !== 0) {
        inputBuffer.copy(buf, TOKEN_HEADER_BYTE_SIZE, inputStart, inputStart + inputLength);
      }

      const paddedLength = pad4(inputLength);
      const additionalLength = pad4(totalLength) - paddedLength;

      buf.fill(0, TOKEN_HEADER_BYTE_SIZE + inputLength, TOKEN_HEADER_BYTE_SIZE + paddedLength);

      return write(fd, buf, 0, TOKEN_HEADER_BYTE_SIZE + paddedLength, position)
      .then(bytesWritten => {
        position += bytesWritten + additionalLength;
        if (additionalLength !== 0) {
          return ftruncate(fd, position).then(() => position);
        }
        else return position;
      });
    });
  }

  findToken(token, position) {
    if ('string' === typeof token) token = tokenToUint32(token);
    return findToken(this.fd, token, position);
  }
}
