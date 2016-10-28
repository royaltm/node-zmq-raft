/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const assert = require('assert');
const { read } = require('fs');

const BYTES_PER_ELEMENT = Uint32Array.BYTES_PER_ELEMENT;

exports.BYTES_PER_ELEMENT = BYTES_PER_ELEMENT;
exports.tokenToUint32 = tokenToUint32;
exports.pad4 = pad4;
exports.findToken = findToken;

function pad4(bytes) {
  return bytes + (-bytes & 3);
}

const tokenBuffer = Buffer.allocUnsafe(BYTES_PER_ELEMENT);
const tokenArray = new Uint32Array(tokenBuffer.buffer, tokenBuffer.byteOffset, 1);

function tokenToUint32(token) {
  assert.equal(token.length, BYTES_PER_ELEMENT);
  tokenBuffer.write(token);
  return tokenArray[0];
}

/**
 * Find next token data in a token file
 *
 * Resolves to [data_segment_offset, data_segment_size]
 *
 * @param {number} fd - file descriptor
 * @param {number} token - unsigned 32-bit LSB integer token
 * @param {number} position - position in a file to start searching
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
