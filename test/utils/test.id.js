/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const { id } = raft.utils;

test('should have functions and properties', t => {
  t.type(id.genIdent                 , 'function');
  t.strictEquals(id.genIdent.length  , 2);
  t.type(id.isIdent                  , 'function');
  t.strictEquals(id.isIdent.length   , 1);
  t.type(id.getSeconds               , 'function');
  t.strictEquals(id.getSeconds.length, 2);
  t.end();
});

test('genIdent', t => {
  t.type(id.genIdent(), 'string');
  t.type(id.genIdent('buffer'), Buffer);
  t.strictEquals(id.genIdent().length, 24);
  t.strictEquals(id.genIdent('buffer').length, 12);
  t.type(id.genIdent('hex'), 'string');
  t.strictEquals(id.genIdent('hex').length, 24);
  t.type(id.genIdent('base64'), 'string');
  t.strictEquals(id.genIdent('base64').length, 16);
  var buf = Buffer.alloc(20);
  for(var sum = 0, i = 0; i < 20; ++i) sum += buf[i];
  t.strictEquals(sum, 0);
  for(var i = 12; i < 20; ++i) buf[i] = i + 100;
  t.strictEquals(id.genIdent(buf), buf);
  for(var i = 12; i < 20; ++i) {
    t.strictEquals(buf[i], i + 100);
  }
  var ary = [];
  for(var sum = 0, i = 0; i < 12; ++i) {
    ary.push(buf[i]);
    sum += buf[i];
  }
  t.ok(sum > 0);
  for(var i = 8; i < 20; ++i) buf[i] = 0;
  t.strictEquals(id.genIdent(buf, 8), buf);
  for(var sum = 0, i = 8; i < 20; ++i) sum += buf[i];
  t.ok(sum > 0);
  var idents = new Set();
  for(var i = 0; i < 1000; ++i) {
    let ident = id.genIdent();
    t.strictEquals(idents.has(ident), false);
    idents.add(ident);
  }
  t.end();
});

test('isIdent', t => {
  t.strictEquals(id.isIdent(), false);
  t.strictEquals(id.isIdent(''), false);
  t.strictEquals(id.isIdent('foo'), false);
  t.strictEquals(id.isIdent(0), false);
  t.strictEquals(id.isIdent(null), false);
  t.strictEquals(id.isIdent(true), false);
  t.strictEquals(id.isIdent(false), false);
  t.strictEquals(id.isIdent({}), false);
  t.strictEquals(id.isIdent([]), false);
  t.strictEquals(id.isIdent(new Date()), false);
  t.strictEquals(id.isIdent(Buffer.alloc(10)), false);
  t.strictEquals(id.isIdent(Buffer.alloc(16)), false);
  t.strictEquals(id.isIdent(Buffer.alloc(0)), false);
  t.strictEquals(id.isIdent('5816b41e26f27e0494708988'), true);
  t.strictEquals(id.isIdent(Buffer.from('5816b41e26f27e0494708988', 'hex')), true);
  t.strictEquals(id.isIdent(id.genIdent('buffer')), true);
  t.strictEquals(id.isIdent(id.genIdent()), true);
  t.strictEquals(id.isIdent(id.genIdent('hex')), true);
  t.end();
});

test('getSeconds', t => {
  t.throws(() => id.getSeconds());
  t.strictEquals(id.getSeconds(Buffer.alloc(12)), 0);
  t.strictEquals(id.getSeconds('5816b41e26f27e0494708988'), 1477882910);
  t.strictEquals(id.getSeconds(Buffer.from('0000005816b41e26f27e0494708988', 'hex'), 3), 1477882910);
  t.strictEquals(id.getSeconds('WBa0HibyfgSUcImI'), 1477882910);
  t.type(id.getSeconds(id.genIdent('buffer')), 'number');
  t.type(id.getSeconds(id.genIdent()), 'number');
  t.type(id.getSeconds(id.genIdent('hex')), 'number');
  t.type(id.getSeconds(id.genIdent('base64')), 'number');
  var time = Date.now() / 1000 >>> 0;
  t.ok(id.getSeconds(id.genIdent('buffer')) >= time);
  t.ok(id.getSeconds(id.genIdent()) >= time);
  t.ok(id.getSeconds(id.genIdent('hex')) >= time);
  t.ok(id.getSeconds(id.genIdent('base64')) >= time);
  t.ok(id.getSeconds(id.genIdent('buffer')) % 1 === 0);
  t.ok(id.getSeconds(id.genIdent()) % 1 === 0);
  t.ok(id.getSeconds(id.genIdent('hex')) % 1 === 0);
  t.ok(id.getSeconds(id.genIdent('base64')) % 1 === 0);
  t.end();
});
