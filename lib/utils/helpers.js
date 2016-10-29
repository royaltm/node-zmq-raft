/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray
    , random = Math.random;

const assert = require('assert');

/* a mischievous place where non strong typed languages live */

/**
 * Validates if all own property keys of the provided `constants` object have defined
 * value and are of type `type`.
 *
 * @param {Object} constants
 * @param {string} type
 * @param {bool} [checkNonFalsy]
 * @return {Object} constants for passing through
**/
exports.assertConstantsDefined = function(constants, type, checkNonFalsy) {
  for(var name in constants) {
    if (constants.hasOwnProperty(name)) {
      assert(constants[name] !== undefined, "missing constant: " + name);
      assert(type === typeof constants[name], "bad constant type: " + name + " - " + typeof constants[name]);
      if (checkNonFalsy) {
        assert(!!constants[name], "falsy constant: " + name);
      }
    }
  }
  return constants;
};

/**
 * Creates function that randomize integer number between two provided values (inclusive).
 *
 * `min` and `max` arguments may be swapped
 *
 * @param {number} min
 * @param {number} max
 * @return {Function}
**/
exports.createRangeRandomizer = function(v0, v1) {
  if (!Number.isFinite(v0) || !Number.isFinite(v1)) throw new TypeError('arguments must be numbers');
  v0 >>= 0;
  v1 >>= 0;
  const min = Math.min(v0, v1);
  const range = Math.max(v0, v1) - min + 1;
  return function() {
    return (random() * range + min)>>0;
  };
};

const defineProperty = Object.defineProperty;
const constDescriptor = { value: undefined, enumerable: true, configurable: false, writable: false };
/**
 * Defines constant property on provided object.
 *
 * @param {Object} target
 * @param {string} property
 * @param {*} value
 * @return value
**/
exports.defineConst = function(target, property, value) {
  constDescriptor.value = value;
  defineProperty(target, property, constDescriptor);
  return value;
};

/**
 * Returns a promise that resolves after `timeout` milliseconds.
 *
 * `result` argument will be resolved
 *
 * @param {number} timeout
 * @param {*} [result]
 * @return {Promise}
**/
exports.delay = function(timeout, result) {
  return new Promise((resolve, reject) => setTimeout(resolve, timeout, result));
};

const spaces = (" ").repeat(256);
/**
 * Returns string padded to size with optional padder.
 *
 * @param {string} input
 * @param {number} size
 * @param {string} [padder]
 * @return {string}
**/
exports.lpad = function(input, size, padder) {
  var strlen = input.length;
  size >>= 0;
  if (strlen >= size) return input;
  if ('string' !== typeof padder) {
    padder = (padder !== undefined ? String(padder) : spaces);
  }
  var padlen = padder.length;
  if (size > padlen) {
    padder = padder.repeat((size + padlen - 1) / padlen >>> 0);
  }
  return padder.substring(0, size - strlen) + input;
}

/**
 * @property {Regexp} regexp that does not match anything.
**/
exports.matchNothingPattern = /^[^\u0000-\uffff]+$/;

/**
 * Returns a map consisting of id -> url pairs.
 *
 * Accepts many input formats:
 *
 * - array of url strings, in this instance id will equal to url
 * - array of [id, url] pairs
 * - array of {id, url} objects (one of the property may be missing in this instance id === url)
 *
 * @param {Array} peers
 * @return {Map}
**/
exports.parsePeers = function(peers) {
  if (!isArray(peers))
    throw TypeError('peers must be an array');

  return peers.reduce((map, peer) => {
    var id;

    if ('string' === typeof peer && peer.length !== 0) {
      id = peer;
    }
    else if (isArray(peer)) {
      id = peer[0];
      peer = peer[1] || id;
    }
    else if (!peer || 'object' !== typeof peer) {
      throw new Error("peer must be an url string or tuple [id, url] or object with 'url' and 'id' properties");
    }
    else {
      id = peer.id || peer.url;
      peer = peer.url || id;
    }
    if (!id || 'string' !== typeof id) throw new Error("peer must have id string that is not empty");
    if (!peer || 'string' !== typeof peer) throw new Error("peer must have url string that is not empty");
    map.set(id, peer);

    return map;
  }, new Map());
};

const escapeRe = /[-\/\\^$*+?.()|[\]{}]/g;
/**
 * Returns exact match regex pattern.
 *
 * @param {string} input
 * @return {string}
**/
exports.regexpEscape = function(input) {
  return input.replace(escapeRe, '\\$&');
};
