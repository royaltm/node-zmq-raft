/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

/*

A token file consists of tagged chunks (segments).
Each chunk has a 8 byte header followed by random data follewed by optional padding.
The chunk header is a 4 byte identifier (token) followed by 4 byte (LSB) length of the data.
The padding is 1 to 3 bytes of zeroes to ensure that each chunk always starts at byte offset
which is a multiplication of 4 bytes.

*/

const assert = require('assert');
const { read: readCb } = require('fs');
const { open, write, ftruncate, close, read } = require('../utils/fsutil');

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
 * Resolves to [data_segment_offset, data_segment_size] on success
 * Resolves to null when stop is provided and token is not found before that position
 *
 * throws error if not found after the end of file
 *
 * @param {number} fd - file descriptor
 * @param {UInt32} token - unsigned 32-bit LSB integer token
 * @param {number} position - position in a file to start searching from
 * @param {number} [stop] - position after which stop searching
 * @return {Promise}
**/
function findToken(fd, token, position, stop) {
  const headerBuf = Buffer.allocUnsafe(2 * BYTES_PER_ELEMENT);
  const header = new Uint32Array(headerBuf.buffer, headerBuf.byteOffset, 2);
  stop = (stop != null) ? parseInt(stop) : Number.POSITIVE_INFINITY;
  return new Promise((resolve, reject) => {
    const seek = () => {
      if (position < stop) {
        readCb(fd, headerBuf, 0, headerBuf.length, position, (err, bytesRead) => {
          if (err) return reject(err);
          if (bytesRead !== headerBuf.length) return reject(new Error("tokenFile: bytesRead mismatch"));

          if (header[0] === token) return resolve([position + headerBuf.length, header[1]]);
          position += headerBuf.length + pad4(header[1]);
          seek();
        });
      }
      else resolve(null);
    };
    seek();
  });
}

/**
 * Creates a new token file ensuring that file does not exists while being created.
 *
 * Promise resolves to TokenFile.
 *
 * @param {string} filename
 * @return {Promise}
**/
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

  /**
   * Appends a new chunk segment to the file.
   *
   * If no inputBuffer is provided or inputLength is less than the dataLength
   * the remaining bytes of the segment is being filled with zeroes.
   *
   * @param {string|UInt32} token
   * @param {number} dataLength
   * @param {Buffer} [inputBuffer]
   * @param {number} [inputStart]
   * @param {number} [inputLength]
   * @return {Promise}
  **/
  appendToken(token, dataLength, inputBuffer, inputStart, inputLength) {
    return this.promise = this.promise.then(position => {
      const fd = this.fd
          , inputBufLength = inputBuffer && inputBuffer.length || 0;

      dataLength = parseInt(dataLength);
      if (!isFinite(dataLength)) dataLength = 0;

      inputStart >>>= 0;
      inputLength = (inputLength == null) ? inputBufLength
                                          : (inputLength >>> 0);

      if (inputStart + inputLength > inputBufLength) {
        inputLength = inputBufLength - inputStart;
        if (inputLength < 0) inputLength = 0;
      }
      if (inputLength > dataLength) inputLength = dataLength;

      if ('string' === typeof token) token = tokenToUint32(token);

      const buf = this.ensureBuffer(inputLength)
          , header = this.header;

      header[0] = token;
      header[1] = dataLength;

      if (inputLength !== 0) {
        inputBuffer.copy(buf, TOKEN_HEADER_BYTE_SIZE, inputStart, inputStart + inputLength);
      }

      const paddedLength = pad4(inputLength);
      const additionalLength = pad4(dataLength) - paddedLength;

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

  /**
   * Find next token data in a token file
   *
   * Resolves to [data_segment_offset, data_segment_size] on success
   * Resolves to null when stop is provided and token is not found before that position
   *
   * throws error if not found after the end of file
   *
   * @param {UInt32|string} token - unsigned 32-bit LSB integer token or 4 byte length ascii string
   * @param {number} position - position in a file to start searching from
   * @param {number} [stop] - position after which stop searching
   * @return {Promise}
  **/
  findToken(token, position, stop) {
    if ('string' === typeof token) token = tokenToUint32(token);
    return findToken(this.fd, token, position, stop);
  }

  /**
   * Read token data
   *
   * Resolves to a buffer containing token data
   * Resolves to null when stop is provided and token is not found before that position
   *
   * throws error if not found
   *
   * @param {UInt32|string} token - unsigned 32-bit LSB integer token or 4 byte length ascii string
   * @param {number} [position] - position in a file to start searching from
   * @param {number} [stop] - position after which stop searching
   * @return {Promise}
  **/
  readTokenData(token, position, stop) {
    return this.findToken(token, parseInt(position) || 0, stop)
    .then(result => {
      if (!result) return null;
      var [position, length] = result
        , buffer = Buffer.allocUnsafe(length);
      return read(this.fd, buffer, 0, length, position)
      .then(() => buffer);
    });
  }

  /**
   * Close token file
  **/
  close() {
    var fd = this.fd;
    if (fd != null) {
      this.fd = null;
      return close(fd);
    }
    else Promise.resolve();
  }
}

exports.TokenFile = TokenFile;
