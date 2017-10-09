/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";
/*

from https://docs.mongodb.com/manual/reference/method/ObjectId/

The 12-byte id value consists of:

- 4-byte value representing the seconds since the Unix epoch (most significant byte first)
- 3-byte machine identifier
- 2-byte process id, and
- 3-byte counter, starting with a random value.

*/

const crypto = require('crypto');
const os = require('os');
const isBuffer = Buffer.isBuffer;
const now = Date.now;

const machineId = crypto.createHash('md5').update(os.hostname()).digest();
const buffer = new Buffer(12);
machineId.copy(buffer, 4, 0, 3);
buffer.writeUInt16BE(process.pid & 0xffff, 7);

var counter = crypto.randomBytes(3).readUIntBE(0, 3);

/**
 * Generates fresh unique id.
 *
 * genIdent([encoding])
 * genIdent(buffer[, offset])
 * 
 * @param {string|Buffer} [encoding] or buffer
 * @param {number} [offset] optional buffer offset
 * @return {string|Buffer}
**/
exports.genIdent = function(encoding, offset) {
  var time = (now()/1000)>>>0;
  buffer.writeUInt32BE(time, 0, true);
  buffer.writeUIntBE(counter++, 9, 3, true);
  counter &= 0xffffff;

  if (isBuffer(encoding)) {
    buffer.copy(encoding, offset>>>0, 0, 12);
    return encoding;
  }
  else return encoding === 'buffer' ? Buffer.from(buffer) : buffer.toString(encoding||'hex');
};

const IDMATCH = /^[0-9a-f]{24}$/;

/**
 * Tests for unique id type.
 * 
 * @param {string|Buffer} value
 * @return {bool}
**/
exports.isIdent = function(value) {
  return (isBuffer(value) && value.length === 12) || ('string' === typeof value && IDMATCH.test(value));
};

const timeBuf = new Buffer(4);

/**
 * Extracts seconds from unique id.
 * 
 * @param {string|Buffer} value
 * @param {number} [offset]
 * @return {number}
**/
exports.getSeconds = function(value, offset) {
  if ('string' === typeof value) {
    var encoding, len = value.length;
    if (len === 16) {
      encoding = 'base64';
    }
    else if (len === 24) {
      encoding = 'hex';
    }
    else throw new TypeError("invalid string value");
    timeBuf.write(value, 0, 12, encoding);
    value = timeBuf;
    offset = 0;
  }
  else offset >>>= 0;
  return value.readUInt32BE(offset, true);
};
