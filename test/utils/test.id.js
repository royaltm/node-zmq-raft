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
  t.equal(id.genIdent.length  , 2);
  t.type(id.isIdent                  , 'function');
  t.equal(id.isIdent.length   , 1);
  t.type(id.getSeconds               , 'function');
  t.equal(id.getSeconds.length, 2);
  t.end();
});

test('genIdent', t => {
  t.type(id.genIdent(), 'string');
  t.type(id.genIdent('buffer'), Buffer);
  t.equal(id.genIdent().length, 24);
  t.equal(id.genIdent('buffer').length, 12);
  t.type(id.genIdent('hex'), 'string');
  t.equal(id.genIdent('hex').length, 24);
  t.type(id.genIdent('base64'), 'string');
  t.equal(id.genIdent('base64').length, 16);
  var buf = Buffer.alloc(20);
  for(var sum = 0, i = 0; i < 20; ++i) sum += buf[i];
  t.equal(sum, 0);
  for(var i = 12; i < 20; ++i) buf[i] = i + 100;
  t.equal(id.genIdent(buf), buf);
  for(var i = 12; i < 20; ++i) {
    t.equal(buf[i], i + 100);
  }
  var ary = [];
  for(var sum = 0, i = 0; i < 12; ++i) {
    ary.push(buf[i]);
    sum += buf[i];
  }
  t.ok(sum > 0);
  for(var i = 8; i < 20; ++i) buf[i] = 0;
  t.equal(id.genIdent(buf, 8), buf);
  for(var sum = 0, i = 8; i < 20; ++i) sum += buf[i];
  t.ok(sum > 0);
  var idents = new Set();
  for(var i = 0; i < 1000; ++i) {
    let ident = id.genIdent();
    t.equal(idents.has(ident), false);
    idents.add(ident);
  }
  t.end();
});

test('isIdent', t => {
  t.equal(id.isIdent(), false);
  t.equal(id.isIdent(''), false);
  t.equal(id.isIdent('foo'), false);
  t.equal(id.isIdent(0), false);
  t.equal(id.isIdent(null), false);
  t.equal(id.isIdent(true), false);
  t.equal(id.isIdent(false), false);
  t.equal(id.isIdent({}), false);
  t.equal(id.isIdent([]), false);
  t.equal(id.isIdent(new Date()), false);
  t.equal(id.isIdent(Buffer.alloc(10)), false);
  t.equal(id.isIdent(Buffer.alloc(16)), false);
  t.equal(id.isIdent(Buffer.alloc(0)), false);
  t.equal(id.isIdent('5816b41e26f27e0494708988'), true);
  t.equal(id.isIdent(Buffer.from('5816b41e26f27e0494708988', 'hex')), true);
  t.equal(id.isIdent(id.genIdent('buffer')), true);
  t.equal(id.isIdent(id.genIdent()), true);
  t.equal(id.isIdent(id.genIdent('hex')), true);
  t.end();
});

test('getSeconds', t => {
  t.throws(() => id.getSeconds());
  t.equal(id.getSeconds(Buffer.alloc(12)), 0);
  t.equal(id.getSeconds('5816b41e26f27e0494708988'), 1477882910);
  t.equal(id.getSeconds(Buffer.from('0000005816b41e26f27e0494708988', 'hex'), 3), 1477882910);
  t.equal(id.getSeconds('WBa0HibyfgSUcImI'), 1477882910);
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
