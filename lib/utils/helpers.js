/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray
    , random = Math.random
    , toString = {}.toString;

const parseUrl = require('url').parse;

const { isIP } = require('net');

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

exports.validatePeerUrlFormat = validatePeerUrlFormat;

function validatePeerUrlFormat(peer) {
  assert(isNonEmptyString(peer), "peer url must be a non empty string");
  var url = parseUrl(peer)
    , port = url.port|0;
  assert.strictEqual(url.protocol, "tcp:", "peer url protocol must be tcp:");
  assert.strictEqual(url.auth, null, "peer url must have no auth");
  assert(url.path === null || (url.path === '/' && !peer.endsWith('/')), "peer url must have no path");
  assert.strictEqual(url.hash, null, "peer url must have no hash");
  assert(port > 0 && port < 0x10000, "peer url port must be in range 1-65535");
  assert.notStrictEqual(url.hostname, "0.0.0.0", "peer url must not be a placeholder address");
  assert.notStrictEqual(url.hostname, "::", "peer url must not be a placeholder address");
  assert(!!isIP(url.hostname), "peer url must have a valid ip address in hostname");
  return url;
}

/**
 * Returns a map consisting of id -> url pairs.
 *
 * Accepts many input formats:
 *
 * - array of url strings, in this instance id will equal to url
 * - array of [id, url] pairs
 * - array of {id, url} objects (one of the property may be missing in this instance id === url)
 *
 * If the oldPeers argument is provided, additionally checks if peers are not conflicting with oldPeers
 *
 * @param {Array} peers
 * @param {Map} [oldPeers]
 * @return {Map}
**/
exports.parsePeers = function(peers, oldPeers) {
  if (!isArray(peers))
    throw TypeError('peers must be an array');

  if (oldPeers !== undefined && !isMap(oldPeers))
    throw TypeError('oldPeers must be an array');

  var result = new Map()
    , urls = new Set()
    , oldUrls;

  if (oldPeers !== undefined) oldUrls = new Set(oldPeers.values());

  peers.forEach(peer => {
    var id;

    if ('string' === typeof peer) {
      id = peer;
    }
    else if (isArray(peer)) {
      id = peer[0];
      peer = peer[1];
      if (peer === undefined) peer = id;
    }
    else {
      assert(peer !== null && 'object' === typeof peer, "peer must be an url string or a tuple [id, url] or an object with 'url' and 'id' properties");
      id = peer.id
      if (id === undefined) id = peer.url;
      peer = peer.url;
      if (peer === undefined) peer = id;
    }
    assert(isNonEmptyString(id), "peer id must be a non empty string");
    assert(!result.has(id), "peer id must be unique");
    validatePeerUrlFormat(peer);
    assert(!urls.has(peer), "peer url must be unique");

    if (oldPeers !== undefined) {
      assert((!oldPeers.has(id) && !oldUrls.has(peer))
        || oldPeers.get(id) === peer,
        'new peers must be consistent with current configuration');
    }

    urls.add(peer);
    result.set(id, peer);
  });

  assert(result.size !== 0, "at least one peer must be defined in a cluster");

  return result;
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

/**
 * Returns true of the input is a non-empty string
 *
 * @param {*} input
 * @return {boolean}
**/
exports.isNonEmptyString = isNonEmptyString;

function isNonEmptyString(input) {
  return 'string' === typeof input && input.length !== 0;
}

/**
 * Calculate majority of the given count
 *
 * @param {number} count
 * @return {number}
**/
exports.majorityOf = function(count) {
  return (count >>> 1) + 1;
};


/**
 * Map type check.
 *
 * @param {*} value The value to check.
 * @return Boolean
 */
exports.isMap = isMap;

function isMap(value) {
  return toString.call(value) === '[object Map]';
}
