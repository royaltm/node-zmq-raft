/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const scopes = new WeakMap();
const emptyFunction = () => {};

/**
 * Synchronizes execution
 *
 * Guarantees that no other callback (over the same scope) is invoked until
 * promises returned by all previous callbacks (one by one) complete (eighter way
 * as resolved or rejected)
 *
 * The order of callbacks will be preserved.
 *
 * WARNING: Make sure you catch all the errors of promises returned by the callback;
 * synchronize will NOT allow you to detect unhandled promises via 'unhandledRejection' event
 *
 * WARNING: susceptible to deadlocks on recursion
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
