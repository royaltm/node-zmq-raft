/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: BSD
 *
 *  Fast and GC friendly converter of any unsigned integer to variable binary
 *
 */
"use strict";

const min = Math.min;
const MAX_ALLOWED_INTEGER = Number.MAX_SAFE_INTEGER || 9007199254740991;
const MIN_ALLOWED_INTEGER = Number.MIN_SAFE_INTEGER || -9007199254740991;
const isFiniteNumber = Number.isFinite || ((n) => 'number' === typeof n && isFinite(n));

const POOL_SIZE   = 65536;
const offsetLimit = POOL_SIZE - 8;
var pool          = Buffer.allocUnsafe(POOL_SIZE);
var poolOffset    = 0;

const NULL_BUFFER = Buffer.alloc(0);
const BUF_TRUE = Buffer.from([1]);
const BUF_FALSE = Buffer.from([]);

exports.MAX_ALLOWED_INTEGER = MAX_ALLOWED_INTEGER;
exports.MIN_ALLOWED_INTEGER = MIN_ALLOWED_INTEGER;

exports.boolToBuffer     = boolToBuffer;
exports.bufferToBool     = bufferToBool;
exports.allocBufUIntLE   = allocBufUIntLE;
exports.writeBufUIntLE   = writeBufUIntLE;
exports.readBufUIntLE    = readBufUIntLE;
exports.allocBufIntLE    = allocBufIntLE;
exports.writeBufIntLE    = writeBufIntLE;
exports.readBufIntLE     = readBufIntLE;
exports.allocBufNumberLE = allocBufNumberLE;
exports.writeBufNumberLE = writeBufNumberLE;
exports.readBufNumberLE  = readBufNumberLE;

function boolToBuffer(value) {
  return value ? BUF_TRUE : BUF_FALSE;
}

function bufferToBool(buffer) {
  return (buffer.length !== 0 && !!buffer[0]);
}

function allocBufWriter(value, writer) {
  if (value == null) return NULL_BUFFER;
  var offset = poolOffset;
  if (offset > offsetLimit) {
    pool = Buffer.allocUnsafe(POOL_SIZE);
    offset = 0;
  }
  return pool.slice(offset, poolOffset = writer(value, pool, offset));  
}

/**
 * Write an unsigned integer to the byte buffer in LSB-first order using variable number of bytes.
 *
 * Returns a new buffer object pointing to the data allocated from internal pool.
 *
 * `value` must be less or equal to MAX_ALLOWED_INTEGER and greater or equal to 0.
 *
 * @param {number|null} value
 * @param {boolean} [typecheck]
 * @return {Buffer}
**/
function allocBufUIntLE(value, typecheck) {
  if (typecheck && (!isFiniteNumber(value) || value < 0 || value % 1 !== 0)) {
    throw new TypeError("value is not an unsigned integer");
  }
  return allocBufWriter(value, writeBufUIntLE);
}

/**
 * Write a signed integer to the byte buffer in LSB-first order using variable number of bytes.
 *
 * Returns a new buffer object pointing to the data allocated from internal pool.
 *
 * `value` must be less or equal to MAX_ALLOWED_INTEGER and greater or equal to MIN_ALLOWED_INTEGER.
 *
 * @param {number|null} value
 * @param {boolean} [typecheck]
 * @return {Buffer}
**/
function allocBufIntLE(value, typecheck) {
  if (typecheck && (!isFiniteNumber(value) || value % 1 !== 0)) {
    throw new TypeError("value is not an integer");
  }
  return allocBufWriter(value, writeBufIntLE);
}

/**
 * Write a number (as integer or double) to the byte buffer in LSB-first order using variable number of bytes.
 *
 * Returns a new buffer object pointing to the data allocated from internal pool.
 *
 * `value` between MIN_ALLOWED_INTEGER and MAX_ALLOWED_INTEGER is written as (1 to 7 byte) integer
 * otherwise it's being written as a 8-byte double.
 *
 * @param {number|null} value
 * @param {boolean} [typecheck]
 * @return {Buffer}
**/
function allocBufNumberLE(value, typecheck) {
  if (typecheck && 'number' !== typeof value) {
    throw new TypeError("value is not a number");
  }
  return allocBufWriter(value, writeBufNumberLE);
}

/**
 * Write unsigned integer to the byte buffer in LSB-first order using variable number of bytes.
 *
 * Returns offset position in the buffer after the last written byte.
 *
 * `value` must be less or equal to MAX_ALLOWED_INTEGER and greater or equal to 0.
 *
 * @param {number} value
 * @param {Buffer} buffer
 * @param {number} [offset] at which to begin writing
 * @return {number}
**/
function writeBufUIntLE(value, buffer, offset) {
  value = +value;
  offset = offset >>> 0;
  if (value > MAX_ALLOWED_INTEGER) throw new Error("value is above maximum allowed integer");

  const limit = buffer.length - 1;
  if (offset > limit) return offset;

  var mul = 0x100;
  var v = buffer[offset] = value;

  if (v >= 0x100) {
    while (offset < limit) {
      buffer[++offset] = v = value / mul;
      if (v < 0x100) break;
      mul *= 0x100;
    }
  }

  return offset + 1;
}

/**
 * Write signed integer to the byte buffer in LSB-first order using variable number of bytes.
 *
 * Returns offset position in the buffer after the last written byte.
 *
 * `value` must be less or equal to MAX_ALLOWED_INTEGER and greater or equal to MIN_ALLOWED_INTEGER.
 *
 * @param {number} value
 * @param {Buffer} buffer
 * @param {number} [offset] at which to begin writing
 * @return {number}
**/
function writeBufIntLE(value, buffer, offset) {
  value = +value;
  offset = offset >>> 0;
  if (value > MAX_ALLOWED_INTEGER) throw new Error("value is above maximum allowed integer");
  if (value < MIN_ALLOWED_INTEGER) throw new Error("value is below minimum allowed integer");

  const limit = buffer.length - 1;
  if (offset > limit) return offset;

  var mul = 0x100;
  var v = buffer[offset] = value;

  if (value < 0) {
    var sub = 0;
    /* negative number */
    if (v < -0x80) {
      while (offset < limit) {
        if (sub === 0 && (v & 0xff) !== 0) sub = 1;
        buffer[++offset] = ((v = value / mul) >> 0) - sub;
        if (v >= -0x80) break;
        mul *= 0x100;
      }
    }
  }
  else {
    /* positive number */
    if (v >= 0x100) {
      while (offset < limit) {
        buffer[++offset] = v = value / mul;
        if (v < 0x100) break;
        mul *= 0x100;
      }
    }
    /* last padding byte to indicate positive integer */
    if (v >= 0x80 && offset < limit) buffer[++offset] = 0;
  }

  return offset + 1;
}


/**
 * Write a number (integer or double) to the byte buffer in LSB-first order using variable number of bytes.
 *
 * Returns offset position in the buffer after the last written byte.
 *
 * `value` between MIN_ALLOWED_INTEGER and MAX_ALLOWED_INTEGER is written as (1 to 7 byte) integer
 * otherwise it's being written as an eight byte double.
 *
 * @param {number} value
 * @param {Buffer} buffer
 * @param {number} [offset] at which to begin writing
 * @return {number}
**/
function writeBufNumberLE(value, buffer, offset) {
  value = +value;
  if (value % 1 === 0 && value <= MAX_ALLOWED_INTEGER && value >= MIN_ALLOWED_INTEGER) {
    return writeBufIntLE(value, buffer, offset);
  }
  return buffer.writeDoubleLE(value, offset, true);
}

/**
 * Read variable size unsigned integer from the buffer in LSB-first order
 *
 * The variable is being read from up to 7 bytes or to the end of the buffer.
 * Returns decoded number or null if buffer after given offset was empty.
 * Supports up to 54 bits of accuracy.
 *
 * @param {Buffer} buffer
 * @param {number} [offset] offset to start reading from
 * @param {number} [stop] offset to stop reading before
 * @return {number|null}
**/
function readBufUIntLE(buffer, offset, stop) {
  offset = offset >>> 0;
  var limit = buffer.length - 1;
  if (stop !== undefined && limit > (stop = (stop >> 0) - 1)) limit = stop;
  if (offset > limit) return null;

  var val = buffer[offset];
  var mul = 1;
  while (offset < limit) {
    val += buffer[++offset] * (mul *= 0x100);
  }
  if (val > MAX_ALLOWED_INTEGER) throw new Error("integer read from a buffer is above maximum allowed");

  return val;
}

/**
 * Read variable size signed integer from the buffer in LSB-first order
 *
 * The variable is being read from up to 7 bytes or to the end of the buffer.
 * Returns decoded number or null if buffer after given offset was empty.
 * Supports up to 54 bits of accuracy.
 *
 * @param {Buffer} buffer
 * @param {number} [offset] offset to start reading from
 * @param {number} [stop] offset to stop reading before
 * @return {number|null}
**/
function readBufIntLE(buffer, offset, stop) {
  offset = offset >>> 0;
  var limit = buffer.length - 1;
  if (stop !== undefined && limit > (stop = (stop >> 0) - 1)) limit = stop;
  if (offset > limit) return null;

  var val = 0, mul = 1;
  while (offset < limit) {
    val += buffer[offset++] * mul;
    mul *= 0x100;
  }

  var sign = buffer[offset];
  if (sign >= 0x80) {
    /* the following addition is 53-bit safe for negative numbers:
       the right side: sign * mul - (mul * 0x100) subtracts two numbers with
       only 6 most significant bits of mantissa filled and is not losing any information;
       e.g. for negative 53-bit integer it brings the result within the range
       of [-(2^53) , -(2^48)] for adding least significant 48 bits from `val`
       without losing any information */
    val += sign * mul - (mul * 0x100);
    if (val < MIN_ALLOWED_INTEGER) throw new Error("integer read from a buffer is below minimum allowed");
  }
  else {
    val += sign * mul;
    if (val > MAX_ALLOWED_INTEGER) throw new Error("integer read from a buffer is above maximum allowed");
  }

  return val;
}

/**
 * Read a number (integer or double) from the byte buffer in LSB-first order.
 *
 * If the remaining bytes to read from buffer is 8 bytes or more it's read as 64-bit double.
 * Otherwise the variable is being read as signed integer from up to 7 bytes or to the end of the buffer.
 * Returns decoded number or null if buffer after given offset was empty.
 *
 * @param {Buffer} buffer
 * @param {number} [offset] offset to start reading from
 * @param {number} [stop] offset to stop reading before
 * @return {number|null}
**/
function readBufNumberLE(buffer, offset, stop) {
  offset = offset >>> 0;
  var length = buffer.length;
  if (stop !== undefined && length > (stop = stop >> 0)) length = stop;
  length -= offset;

  if (length < 8) return readBufIntLE(buffer, offset, stop);
  if (length !== 8) throw new Error("byte length of a stored number is above 8 bytes");

  return buffer.readDoubleLE(offset, true);
}

/*
const ben=require('ben');
var b=Buffer.alloc(10)
b.fill(0);
const MAX_ALLOWED_INTEGER = require('./raft/bufconv').MAX_ALLOWED_INTEGER;
const MIN_ALLOWED_INTEGER = require('./raft/bufconv').MIN_ALLOWED_INTEGER;
var {allocBufUIntLE} = require('./raft/bufconv');
var {allocBufIntLE} = require('./raft/bufconv');
var {allocBufNumberLE} = require('./raft/bufconv');
var {writeBufUIntLE} = require('./raft/bufconv');
var {readBufUIntLE}  = require('./raft/bufconv');
var {writeBufIntLE}  = require('./raft/bufconv');
var {readBufIntLE}  = require('./raft/bufconv');
var {readBufNumberLE}  = require('./raft/bufconv');
ben(10000000,()=>{writeBufIntLE(MAX_ALLOWED_INTEGER,b)})
ben(10000000,()=>{b.writeIntLE(MAX_ALLOWED_INTEGER,0,7)})
ben(10000000,()=>{readBufIntLE(b,0,7)})
ben(10000000,()=>{b.readIntLE(0,7)})
ben(10000000,()=>{writeBufIntLE(MIN_ALLOWED_INTEGER,b)})
ben(10000000,()=>{b.writeIntLE(MIN_ALLOWED_INTEGER,0,7)})
ben(10000000,()=>{readBufIntLE(b,0,7)})
ben(10000000,()=>{b.readIntLE(0,7)})
ben(10000000,()=>{allocBufUIntLE(MAX_ALLOWED_INTEGER)})
ben(10000000,()=>{writeBufUIntLE(MAX_ALLOWED_INTEGER,b,0)})
ben(10000000,()=>{b.writeUIntLE(MAX_ALLOWED_INTEGER,0,7)})
ben(10000000,()=>{readBufUIntLE(b,0,7)})
ben(10000000,()=>{b.readUIntLE(0,8)})
ben(10000000,()=>{writeBufUIntLE(1,b,0)})
ben(10000000,()=>{b.writeUIntLE(1,0,1)})
ben(10000000,()=>{readBufUIntLE(b,0,1)})
ben(10000000,()=>{b.readUIntLE(0,1)})
*/
