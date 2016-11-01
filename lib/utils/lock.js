/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const lockedScopes = new WeakMap();
const passThrough = () => {};

/**
 * Shared lock asynchronous call
 *
 * Guarantees that no other exclusive lock callback (over the same scope) is invoked until
 * promises returned by all previous callbacks complete (eighter way as resolved or rejected)
 *
 * The order of callbacks will be preserved.
 *
 * WARNING: Make sure you catch all the errors of promises returned by the callback;
 * lock will NOT allow you to detect unhandled promises via 'unhandledRejection' event
 *
 * WARNING: susceptible to deadlocks on recursion
 *
 * `scope` argument must not be a primitive, it will not be modified in any way
 *         and is only used as a key to the WeakMap
 * `callback` argument function may return promise
 *
 * @param {Object} scope
 * @param {Function} callback
 * @return {Promise}
**/
exports.shared = function(scope, callback) {
  var promise, lock = lockedScopes.get(scope);

  if (lock === undefined) {
    promise = Promise.resolve().then(callback);
    lockedScopes.set(scope, {exclusive: promise, shared: Promise.resolve()});
  }
  else {
    promise = lock.shared;
    if (promise === null) {
      lock.shared = promise = lock.exclusive;
      lock.exclusive = promise = promise.then(callback, callback);
    }
    else {
      promise = promise.then(callback, callback);
      lock.exclusive = Promise.all([
        lock.exclusive.catch(passThrough), promise.catch(passThrough)
      ]);
    }
  }

  return promise;
};

/**
 * Exclusive lock asynchronous call
 *
 * Guarantees that no other (exclusive nor shared) lock callback (over the same scope)
 * is invoked until promises returned by all previous callbacks complete (eighter way
 * as resolved or rejected)
 *
 * The order of callbacks will be preserved.
 *
 * WARNING: Make sure you catch all the errors of promises returned by the callback;
 * lock will NOT allow you to detect unhandled promises via 'unhandledRejection' event
 *
 * WARNING: susceptible to deadlocks on recursion
 *
 * `scope` argument must not be a primitive, it will not be modified in any way
 *         and is only used as a key to the WeakMap
 * `callback` argument function may return promise
 *
 * @param {Object} scope
 * @param {Function} callback
 * @return {Promise}
**/
exports.exclusive = function(scope, callback) {
  var promise, lock = lockedScopes.get(scope);

  if (lock === undefined) {
    promise = Promise.resolve().then(callback);
    lockedScopes.set(scope, {exclusive: promise, shared: null});
  }
  else {
    lock.shared = null;
    promise = lock.exclusive = lock.exclusive.then(callback, callback);
  }

  return promise;
};
