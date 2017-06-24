/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const crypto = require('crypto');
const raft = require('../..');
const { helpers } = raft.utils;

test('should have functions and properties', t => {
  t.type(helpers.assertConstantsDefined                , 'function');
  t.strictEquals(helpers.assertConstantsDefined.length , 3);
  t.type(helpers.createRangeRandomizer                 , 'function');
  t.strictEquals(helpers.createRangeRandomizer.length  , 2);
  t.type(helpers.defineConst                           , 'function');
  t.strictEquals(helpers.defineConst.length            , 3);
  t.type(helpers.delay                                 , 'function');
  t.strictEquals(helpers.delay.length                  , 2);
  t.type(helpers.lpad                                  , 'function');
  t.strictEquals(helpers.lpad.length                   , 3);
  t.type(helpers.matchNothingPattern                   , RegExp);
  t.type(helpers.parsePeers                            , 'function');
  t.strictEquals(helpers.parsePeers.length             , 1);
  t.type(helpers.regexpEscape                          , 'function');
  t.strictEquals(helpers.regexpEscape.length           , 1);
  t.end();
});

test('assertConstantsDefined', t => {
  var o = {};
  t.strictEquals(helpers.assertConstantsDefined(o), o);
  o = {foo: 1, bar: 2, baz: 0/0};
  t.strictEquals(helpers.assertConstantsDefined(o, 'number'), o);
  o = {foo: 'foo', bar: '', baz: ' '};
  t.strictEquals(helpers.assertConstantsDefined(o, 'string'), o);
  o = {foo: Symbol('foo'), bar: Symbol(), baz: Symbol.for('baz')};
  t.strictEquals(helpers.assertConstantsDefined(o, 'symbol'), o);
  o = {foo: 1, bar: 2, baz: undefined};
  t.throws(() => helpers.assertConstantsDefined(o, 'number'));
  o = {foo: 1, bar: 2, baz: '3'};
  t.throws(() => helpers.assertConstantsDefined(o, 'number'));
  o = {foo: 'foo', bar: '', baz: null};
  t.throws(() => helpers.assertConstantsDefined(o, 'string'));
  o = {foo: Symbol('foo'), bar: Symbol(), baz: 'baz'};
  t.throws(() => helpers.assertConstantsDefined(o, 'symbol'));
  t.end();
});

test('createRangeRandomizer', t => {
  t.throws(() => helpers.createRangeRandomizer(), new TypeError('arguments must be numbers'));
  t.type(helpers.createRangeRandomizer(0,0), 'function');
  t.strictEquals(helpers.createRangeRandomizer(0,0).length, 0);
  var fun = helpers.createRangeRandomizer(1, 10);
  var res = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0, 10: 0};
  for(var val, i = 10000; i-- > 0; res[val]++) {
    val = fun();
    t.type(val, 'number');
    t.ok(isFinite(val));
    t.ok(val >= 1);
    t.ok(val <= 10);
    t.strictEquals(val % 1, 0);
  }
  for(var n in res) {
    t.ok(res[n] / 1000 > 0.8);
    t.ok(res[n] / 1000 < 1.2);
  }
  t.end();
});

test('defineConst', t => {
  var o = {}, x = [];
  t.strictEquals(helpers.defineConst(o, 'foo', x), x);
  t.strictEquals(o.foo, x);
  t.throws(() => o.foo = null, TypeError);
  t.strictEquals(o.foo, x);
  t.throws(() => { delete o.foo; }, TypeError);
  t.strictEquals(o.foo, x);
  t.throws(() => helpers.defineConst(o, 'foo', 0), TypeError);
  t.strictEquals(o.foo, x);
  t.end();
});

test('delay', t => {
  t.plan(5);
  var start = Date.now();
  return Promise.all([
    helpers.delay(25).then(res => {
      var time = Date.now() - start;
      t.ok(time >= 24, 'was: ' + time);
      t.strictEquals(res, void 0);
    }),
    helpers.delay(35, Symbol.for('foo')).then(res => {
      var time = Date.now() - start;
      t.ok(time >= 34, 'was: ' + time);
      t.strictEquals(res, Symbol.for('foo'));
    }),
  ]).then(() => t.ok(true)).catch(t.throws);
});

test('lpad', t => {
  t.strictEquals(helpers.lpad(''), '');
  t.strictEquals(helpers.lpad('foo'), 'foo');
  t.strictEquals(helpers.lpad('foo', 1), 'foo');
  t.strictEquals(helpers.lpad('foo', 3), 'foo');
  t.strictEquals(helpers.lpad('foo', 4), ' foo');
  t.strictEquals(helpers.lpad('foo', 10), '       foo');
  t.strictEquals(helpers.lpad('foo', 10, '='), '=======foo');
  t.strictEquals(helpers.lpad('foo', 10, '*+'), '*+*+*+*foo');
  t.strictEquals(helpers.lpad('', 10, '*+'), '*+*+*+*+*+');
  t.end();
});

test('matchNothingPattern', t => {
  t.strictEquals(helpers.matchNothingPattern.test(), false);
  t.strictEquals(helpers.matchNothingPattern.test(null), false);
  t.strictEquals(helpers.matchNothingPattern.test([]), false);
  t.strictEquals(helpers.matchNothingPattern.test({}), false);
  t.strictEquals(helpers.matchNothingPattern.test(0), false);
  t.strictEquals(helpers.matchNothingPattern.test(1), false);
  t.strictEquals(helpers.matchNothingPattern.test(true), false);
  t.strictEquals(helpers.matchNothingPattern.test(false), false);
  t.strictEquals(helpers.matchNothingPattern.test(' '), false);
  t.strictEquals(helpers.matchNothingPattern.test('\x00'), false);
  t.strictEquals(helpers.matchNothingPattern.test('foo'), false);
  t.strictEquals(helpers.matchNothingPattern.test(crypto.randomBytes(10000).toString()), false);
  t.end();
});

test('parsePeers', t => {
  var map;
  t.throws(() => helpers.parsePeers(), TypeError);
  t.throws(() => helpers.parsePeers({}), TypeError);
  t.throws(() => helpers.parsePeers(0), TypeError);
  t.throws(() => helpers.parsePeers(''), TypeError);
  t.throws(() => helpers.parsePeers(['']), TypeError);
  t.throws(() => helpers.parsePeers([['']]), TypeError);
  t.throws(() => helpers.parsePeers([['','x']]), TypeError);
  t.throws(() => helpers.parsePeers([['x','']]), TypeError);
  t.throws(() => helpers.parsePeers([{id:''}]), TypeError);
  t.throws(() => helpers.parsePeers([{url:''}]), TypeError);
  t.throws(() => helpers.parsePeers([{id: 'foo', url:''}]), TypeError);
  t.type(map = helpers.parsePeers([]), Map);
  t.strictEquals(map.size, 0);

  t.type(map = helpers.parsePeers([['foo']]), Map);
  t.strictEquals(map.size, 1);
  t.deepEquals(Array.from(map), [['foo', 'foo']]);

  t.type(map = helpers.parsePeers(['foo', 'bar', 'baz']), Map);
  t.strictEquals(map.size, 3);
  t.deepEquals(Array.from(map), [['foo', 'foo'], ['bar', 'bar'], ['baz', 'baz']]);

  t.type(map = helpers.parsePeers([['foo'], ['bar'], ['baz']]), Map);
  t.strictEquals(map.size, 3);
  t.deepEquals(Array.from(map), [['foo', 'foo'], ['bar', 'bar'], ['baz', 'baz']]);

  t.type(map = helpers.parsePeers([{id:'foo'}, {id:'bar'}, {id:'baz'}]), Map);
  t.strictEquals(map.size, 3);
  t.deepEquals(Array.from(map), [['foo', 'foo'], ['bar', 'bar'], ['baz', 'baz']]);

  t.type(map = helpers.parsePeers([{url:'foo'}, {url:'bar'}, {url:'baz'}]), Map);
  t.strictEquals(map.size, 3);
  t.deepEquals(Array.from(map), [['foo', 'foo'], ['bar', 'bar'], ['baz', 'baz']]);

  t.type(map = helpers.parsePeers([['01','foo'], ['02', 'bar'], ['03', 'baz']]), Map);
  t.strictEquals(map.size, 3);
  t.deepEquals(Array.from(map), [['01','foo'], ['02', 'bar'], ['03', 'baz']]);

  t.type(map = helpers.parsePeers([{id:'01',url:'foo'}, {id:'02', url:'bar'}, {id:'03', url:'baz'}]), Map);
  t.strictEquals(map.size, 3);
  t.deepEquals(Array.from(map), [['01','foo'], ['02', 'bar'], ['03', 'baz']]);
  t.end();
});

test('regexpEscape', t => {
  t.strictEquals(helpers.regexpEscape(''), '');
  t.ok(new RegExp(helpers.regexpEscape('')).test(''));
  t.strictEquals(helpers.regexpEscape('foo'), 'foo');
  t.ok(new RegExp(helpers.regexpEscape('foo')).test('foo'));
  t.strictEquals(helpers.regexpEscape('*'), '\\*');
  t.ok(new RegExp(helpers.regexpEscape('*')).test('*'));
  t.ok(new RegExp(helpers.regexpEscape('-/\\^$*+?.()|[]{}')).test('-/\\^$*+?.()|[]{}'));
  t.end();
});

test('isNonEmptyString', t => {
  t.strictEquals(helpers.isNonEmptyString(), false);
  t.strictEquals(helpers.isNonEmptyString(''), false);
  t.strictEquals(helpers.isNonEmptyString([]), false);
  t.strictEquals(helpers.isNonEmptyString(0), false);
  t.strictEquals(helpers.isNonEmptyString(1), false);
  t.strictEquals(helpers.isNonEmptyString({}), false);
  t.strictEquals(helpers.isNonEmptyString(new Date), false);
  t.strictEquals(helpers.isNonEmptyString(Date), false);
  t.strictEquals(helpers.isNonEmptyString(' '), true);
  t.strictEquals(helpers.isNonEmptyString('1'), true);
  t.strictEquals(helpers.isNonEmptyString('0'), true);
  t.strictEquals(helpers.isNonEmptyString('foo'), true);
  t.end();
});

test('majorityOf', t => {
  t.strictEquals(helpers.majorityOf(0), 1);
  t.strictEquals(helpers.majorityOf(1), 1);
  t.strictEquals(helpers.majorityOf(2), 2);
  t.strictEquals(helpers.majorityOf(3), 2);
  t.strictEquals(helpers.majorityOf(4), 3);
  t.strictEquals(helpers.majorityOf(5), 3);
  t.strictEquals(helpers.majorityOf(6), 4);
  t.strictEquals(helpers.majorityOf(7), 4);
  t.strictEquals(helpers.majorityOf(8), 5);
  t.strictEquals(helpers.majorityOf(9), 5);
  t.strictEquals(helpers.majorityOf(10), 6);
  t.end();
});
