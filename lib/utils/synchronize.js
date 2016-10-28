/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const scopes = new WeakMap();
const emptyFunction = () => {};

// TODO: introduce deadlock timeouts somehow
/**
 * Synchronizes execution
 *
 * Guarantees that no other callback (over the same scope) is invoked until
 * promises returned by all previous callbacks (one by one) complete (eighter way
 * as resolved or rejected)
 *
 * WARNING: Make sure you catch all the errors of your promises; synchronize
 * will NOT allow you to detect unhandled promises via 'unhandledRejection' event
 *
 * WARNING: susceptible to deadlocks if nested
 *
 * `scope` argument must not be a primitive, it will not be modified in any way
 *         and is only used as a key to the WeakMap
 * `callback` argument function may return promise
 *
 * @param {Object} scope
 * @param {Function} [callback]
 * @return {Promise}
**/
module.exports = exports = function synchronize(scope, callback) {
  var promise = scopes.get(scope);

  callback || (callback = emptyFunction);

  if (promise === undefined) {
    promise = Promise.resolve().then(callback);
  }
  else {
    promise = promise.then(callback, callback);
  }

  scopes.set(scope, promise);

  return promise;
};

/*

var synchronize = require('./raft/synchronize').synchronize
var synchronize2 = require('./raft/synchronize').synchronize2
var o = {};
var i = 0
var synchronize = require('./raft/synchronize').synchronize
synchronize({}, () => {var x = ++i; console.log('invoking %s', x); return new Promise((r,e)=>setTimeout(r,10000)).then(()=>console.log('wow %s', x));});
synchronize({}, () => {console.log('invoking'); return new Promise((r,e)=>setTimeout(r,6000)).then(()=>console.log('wow'));});
global.gc();global.gc();global.gc();global.gc();global.gc();global.gc();
global.gc();global.gc();global.gc();global.gc();global.gc();global.gc();
global.gc();global.gc();global.gc();global.gc();global.gc();global.gc();
global.gc();global.gc();global.gc();global.gc();global.gc();global.gc();

ben = require('ben');
ben(100000, () => synchronize(o, () => Promise.resolve()))
ben.async(100000, cb => synchronize(o, () => Promise.resolve()).then(cb,cb), res=>console.log('FIN: %s', res))
ben.async(100000, cb => Promise.all([synchronize(o, () => Promise.resolve()),synchronize(o, () => Promise.resolve()),synchronize(o, () => Promise.resolve())]).then(cb,cb), res=>console.log('FIN: %s', res))
ben.async(100000, cb => Promise.all([synchronize2(o, () => Promise.resolve()),synchronize2(o, () => Promise.resolve()),synchronize2(o, () => Promise.resolve())]).then(cb,cb), res=>console.log('FIN: %s', res))

*/
