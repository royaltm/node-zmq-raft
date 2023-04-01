/*
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const path = require('path');
const fs = require('fs');
const raft = require('../..');
const { FilePersistence } = raft.common;

var workdir = fs.mkdtempSync(path.resolve(__dirname, '..', '..', 'tmp') + path.sep);

process.on('exit', () => {
  fs.readdirSync(workdir).forEach(file => fs.unlinkSync(path.join(workdir, file)));
  fs.rmdirSync(workdir);
});

test('should be a function', t => {
  t.type(FilePersistence, 'function');
  t.end();
});

test('FilePersistence', suite => {

  suite.test('test new persistence', t => {
    t.plan(28);
    var persistence = new FilePersistence(path.join(workdir, 'one.persist'), {foo: 1, bar: 'baz', baz: null});
    t.type(persistence, FilePersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.equal(persistence.foo, 1);
      t.equal(persistence.bar, 'baz');
      t.equal(persistence.baz, null);
      t.same(persistence[Symbol.for('byteSize')], 0);
      t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
      t.same(persistence[Symbol.for('data')], {foo: 1, bar: 'baz', baz: null});

      return persistence.update({foo: 2});
    }).then(() => {
      t.equal(persistence.foo, 2);
      t.equal(persistence.bar, 'baz');
      t.equal(persistence[Symbol.for('byteSize')], 6);
      t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
      t.same(persistence[Symbol.for('data')], {foo: 2, bar: 'baz', baz: null});

      return persistence.update({baz: [1,2,3]});
    }).then(() => {
      t.equal(persistence.foo, 2);
      t.equal(persistence.bar, 'baz');
      t.same(persistence.baz, [1,2,3]);
      t.equal(persistence[Symbol.for('byteSize')], 15);
      t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
      t.same(persistence[Symbol.for('data')], {foo: 2, bar: 'baz', baz: [1,2,3]});

      return Promise.all([
        persistence.update({foo: 42, foe: 'fee'}),
        persistence.update({baz: [1,2,3,4]})
      ]);
    }).then(() => {
      t.equal(persistence.foo, 42);
      t.equal(persistence.bar, 'baz');
      t.same(persistence.baz, [1,2,3,4]);
      t.equal(persistence.foe, undefined);
      t.equal(persistence[Symbol.for('byteSize')], 31);
      t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
      t.same(persistence[Symbol.for('data')], {foo: 42, bar: 'baz', baz: [1,2,3,4]});

      return persistence.close();
    }).then(() => {
      t.equal(fs.statSync(persistence.filename).size, 31);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.test('test existing persistence', t => {
    t.plan(18);
    var persistence = new FilePersistence(path.join(workdir, 'one.persist'), {foo: 1, bar: 'baz', baz: null});
    t.type(persistence, FilePersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.equal(persistence.foo, 42);
      t.equal(persistence.bar, 'baz');
      t.same(persistence.baz, [1,2,3,4]);
      t.equal(persistence.foe, undefined);
      t.equal(persistence[Symbol.for('byteSize')], 31);
      t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
      t.same(persistence[Symbol.for('data')], {foo: 42, bar: 'baz', baz: [1,2,3,4]});

      return persistence.update({foe: 'oof', foo: 7});
    }).then(() => {
      t.equal(persistence.foo, 7);
      t.equal(persistence.bar, 'baz');
      t.same(persistence.baz, [1,2,3,4]);
      t.equal(persistence.foe, undefined);
      t.equal(persistence[Symbol.for('byteSize')], 37);
      t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
      t.same(persistence[Symbol.for('data')], {foo: 7, bar: 'baz', baz: [1,2,3,4]});

      return persistence.close();
    }).then(() => {
      t.equal(fs.statSync(persistence.filename).size, 37);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.test('test rotate', t => {
    t.plan(18);
    var persistence = new FilePersistence(path.join(workdir, 'one.persist'), {foo: 1, bar: 'baz', baz: null});
    t.type(persistence, FilePersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.equal(persistence.foo, 7);
      t.equal(persistence.bar, 'baz');
      t.same(persistence.baz, [1,2,3,4]);
      t.equal(persistence.foe, undefined);
      t.equal(persistence[Symbol.for('byteSize')], 37);
      t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
      t.same(persistence[Symbol.for('data')], {foo: 7, bar: 'baz', baz: [1,2,3,4]});

      return persistence.rotate({foo: 123456});
    }).then(() => {
      t.equal(persistence.foo, 123456);
      t.equal(persistence.bar, 'baz');
      t.same(persistence.baz, [1,2,3,4]);
      t.equal(persistence.foe, undefined);
      t.equal(persistence[Symbol.for('byteSize')], 27);
      t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
      t.same(persistence[Symbol.for('data')], {foo: 123456, bar: 'baz', baz: [1,2,3,4]});

      return persistence.close();
    }).then(() => {
      t.equal(fs.statSync(persistence.filename).size, 27);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });


  suite.test('test auto rotate', t => {
    t.plan(18);
    var bigString = Buffer.allocUnsafe(64*1024).toString('hex');
    var persistence = new FilePersistence(path.join(workdir, 'one.persist'), {foo: 1, bar: 'baz', baz: null});
    t.type(persistence, FilePersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.equal(persistence.foo, 123456);
      t.equal(persistence.bar, 'baz');
      t.same(persistence.baz, [1,2,3,4]);
      t.equal(persistence.foe, undefined);
      t.equal(persistence[Symbol.for('byteSize')], 27);
      t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
      t.same(persistence[Symbol.for('data')], {foo: 123456, bar: 'baz', baz: [1,2,3,4]});

      var bytesize = 0;
      var expectedFoo = 123456 + 5;
      var next = () => {
        if (persistence[Symbol.for('byteSize')] > bytesize) {
          bytesize = persistence[Symbol.for('byteSize')];
          return persistence.update({foo: persistence.foo + 1, bar: bigString}).then(next);
        } else {
          t.equal(persistence.foo, expectedFoo);
          t.equal(persistence.bar, bigString);
          t.same(persistence.baz, [1,2,3,4]);
          t.equal(persistence.foe, undefined);
          t.equal(persistence[Symbol.for('byteSize')], 131100);
          t.same(persistence.defaultData, {foo: 1, bar: 'baz', baz: null});
          t.same(persistence[Symbol.for('data')], {foo: expectedFoo, bar: bigString, baz: [1,2,3,4]});
          return persistence.close();
        }
      };
      return next();
    }).then(() => {
      t.equal(fs.statSync(persistence.filename).size, 131100);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.end();
});
