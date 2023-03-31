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
  t.equal(synchronize.length, 2);
  t.end();
});

test('synchronize', t => {
  t.plan(18);
  var scope = {};
  t.type(synchronize(scope, () => t.ok(true)), Promise);
  var start = Date.now();
  return Promise.all([
    synchronize(scope, () => delay(100).then(() => {
      t.equal(scope.tap, undefined);
      return scope.tap = 1;
    })).then(x => {
      t.equal(x, 1);
    }),

    synchronize(scope, () => Promise.reject("baaa"))
    .catch(msg => {
      t.equal(msg, "baaa");
    }),

    synchronize(scope, () => new Promise((resolve, reject) => setImmediate(() => {
      t.equal(scope.tap, 1);
      scope.tap = 2;
      resolve(2);
    }))).then(x => {
      t.equal(x, 2);
    }),

    synchronize(scope, () => delay(10).then(() => {
      t.equal(scope.tap, 2);
      return scope.tap = 3;
    })).then(x => {
      t.equal(x, 3);
    }),

    synchronize(scope, () => {
      t.equal(scope.tap, 3);
      return scope.tap = 4;
    }).then(x => {
      t.equal(x, 4);
    }),

    synchronize(scope, () => {
      t.equal(scope.tap, 4);
      throw new Error("foo");
    }).catch(err => {
      t.type(err, Error);
      t.equal(err.message, "foo");
    }),

    synchronize(scope).then(() => {
      t.equal(scope.tap, 4);
      scope.tap = 5;
      return delay(0).then(() => {
        scope.tap = 6;
      });
    }),

    synchronize(scope, () => {
      t.equal(scope.tap, 5);
    })
  ])
  .then(() => {
    var delta = Date.now() - start;
    t.equal(scope.tap, 6);
    t.ok(delta > 111, 'not ok, was: ' + delta);
  })
  .catch(t.threw);
});
