/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const { delay } = raft.utils.helpers;
const { synchronize } = raft.utils;

test('should be a function', t => {
  t.type(synchronize, 'function');
  t.strictEquals(synchronize.length, 2);
  t.end();
});

test('synchronize', t => {
  t.plan(18);
  var scope = {};
  t.type(synchronize(scope, () => t.ok(true)), Promise);
  var start = Date.now();
  return Promise.all([
    synchronize(scope, () => delay(100).then(() => {
      t.strictEquals(scope.tap, undefined);
      return scope.tap = 1;
    })).then(x => {
      t.strictEquals(x, 1);
    }),

    synchronize(scope, () => Promise.reject("baaa"))
    .catch(msg => {
      t.strictEquals(msg, "baaa");
    }),

    synchronize(scope, () => new Promise((resolve, reject) => setImmediate(() => {
      t.strictEquals(scope.tap, 1);
      scope.tap = 2;
      resolve(2);
    }))).then(x => {
      t.strictEquals(x, 2);
    }),

    synchronize(scope, () => delay(10).then(() => {
      t.strictEquals(scope.tap, 2);
      return scope.tap = 3;
    })).then(x => {
      t.strictEquals(x, 3);
    }),

    synchronize(scope, () => {
      t.strictEquals(scope.tap, 3);
      return scope.tap = 4;
    }).then(x => {
      t.strictEquals(x, 4);
    }),

    synchronize(scope, () => {
      t.strictEquals(scope.tap, 4);
      throw new Error("foo");
    }).catch(err => {
      t.type(err, Error);
      t.strictEquals(err.message, "foo");
    }),

    synchronize(scope).then(() => {
      t.strictEquals(scope.tap, 4);
      scope.tap = 5;
      return delay(0).then(() => {
        scope.tap = 6;
      });
    }),

    synchronize(scope, () => {
      t.strictEquals(scope.tap, 5);
    })
  ])
  .then(() => {
    var delta = Date.now() - start;
    t.strictEquals(scope.tap, 6);
    t.ok(delta > 111, 'not ok, was: ' + delta);
  })
  .catch(t.threw);
});
