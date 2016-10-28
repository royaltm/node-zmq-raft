/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray
    , random = Math.random;

const assert = require('assert');

exports.createRangeRandomizer = function(v0, v1) {
  if (!Number.isFinite(v0) || !Number.isFinite(v1)) throw new TypeError('timeout must be a number');

  const min = Math.min(v0, v1);
  const range = Math.max(v0, v1) - min;

  return function() {
    return (random() * range + min)>>>0;
  };
};

exports.delay = function(timeout) {
  return new Promise((resolve, reject) => setTimeout(resolve, timeout));
};

/* a mischievous place where non strong typed languages live */
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

const defineProperty = Object.defineProperty;
const constDescriptor = { value: undefined, enumerable: true, configurable: false, writable: false };

exports.defineConst = function(object, property, value) {
  constDescriptor.value = value;
  defineProperty(object, property, constDescriptor);
  return value;
};

const escapeRe = /[-\/\\^$*+?.()|[\]{}]/g;
exports.regexpEscape = function(string) {
  return string.replace(escapeRe, '\\$&');
};

exports.regexpAlwaysNegative = /^[^\u0000-\uffff]+$/;

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
