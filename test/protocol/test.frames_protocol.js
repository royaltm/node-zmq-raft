/*
 *  Copyright (c) 2016-2023 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const FramesProtocol = raft.protocol.FramesProtocol;
const { ZmqRpcSocket, RpcCancelError } = raft.server.ZmqRpcSocket;
const { ZmqSocket } = raft.utils.zmqsocket;
const { ZMQ_LINGER } = require('zeromq');

test('should be a function', t => {
  t.type(FramesProtocol, 'function');
  t.end();
});

test('FramesProtocol', suite => {

  suite.test('empty schema', t => {
    var protocol = new FramesProtocol([], []);
    t.same(protocol.encodeRequest(), []);
    t.same(protocol.encodeRequest(''), []);
    t.same(protocol.encodeRequest([]), []);
    t.same(protocol.encodeRequest(['']), []);
    t.same(protocol.decodeRequest([]), []);
    t.same(protocol.decodeRequest(['']), []);
    t.same(protocol.encodeResponse(), []);
    t.same(protocol.encodeResponse(''), []);
    t.same(protocol.encodeResponse([]), []);
    t.same(protocol.encodeResponse(['']), []);
    t.same(protocol.decodeResponse([]), []);
    t.same(protocol.decodeResponse(['']), []);

    var protocol = new FramesProtocol([], [], {extraArgs: true});
    t.same(protocol.encodeRequest(), []);
    t.same(protocol.encodeRequest(''), ['']);
    t.same(protocol.encodeRequest([]), []);
    t.same(protocol.encodeRequest(['']), ['']);
    t.same(protocol.decodeRequest([]), []);
    t.same(protocol.decodeRequest(['']), ['']);
    t.same(protocol.encodeResponse(), []);
    t.same(protocol.encodeResponse(''), ['']);
    t.same(protocol.encodeResponse([]), []);
    t.same(protocol.encodeResponse(['']), ['']);
    t.same(protocol.decodeResponse([]), []);
    t.same(protocol.decodeResponse(['']), ['']);
    t.end();
  });

  suite.test('one argument schema', t => {
    var protocol = new FramesProtocol(['string'], ['number']);
    t.same(protocol.encodeRequest(), []);
    t.same(protocol.encodeRequest([]), []);
    t.same(protocol.encodeRequest(''), [Buffer.alloc(0)]);
    t.same(protocol.encodeRequest('foo'), [Buffer.from('foo')]);
    var buf = Buffer.from('bar');
    t.same(protocol.encodeRequest(buf), [Buffer.from('bar')]);
    t.equal(protocol.encodeRequest(buf)[0], buf);
    t.same(protocol.encodeRequest(0), [Buffer.from('0')]);
    t.same(protocol.decodeRequest([]), [undefined]);
    t.same(protocol.decodeRequest([Buffer.alloc(0)]), ['']);
    t.same(protocol.decodeRequest([Buffer.from('foo')]), ['foo']);
    t.same(protocol.decodeRequest([Buffer.from('0')]), ['0']);
    t.same(protocol.encodeResponse(), []);
    t.same(protocol.encodeResponse([]), []);
    t.same(protocol.encodeResponse(0), [Buffer.from([0])]);
    t.same(protocol.encodeResponse([0]), [Buffer.from([0])]);
    t.throws(() => protocol.encodeResponse('0'), new TypeError('value is not a number'));
    t.throws(() => protocol.encodeResponse(['0']), new TypeError('value is not a number'));
    var buf = Buffer.from('bar');
    t.same(protocol.encodeResponse(buf), [Buffer.from('bar')]);
    t.equal(protocol.encodeResponse(buf)[0], buf);
    t.same(protocol.encodeResponse(Number.MAX_SAFE_INTEGER), [Buffer.from([255,255,255,255,255,255,31])]);
    t.same(protocol.encodeResponse([Number.MAX_SAFE_INTEGER]), [Buffer.from([255,255,255,255,255,255,31])]);
    t.same(protocol.encodeResponse(Number.MIN_SAFE_INTEGER), [Buffer.from([1,0,0,0,0,0,224])]);
    t.same(protocol.encodeResponse([Number.MIN_SAFE_INTEGER]), [Buffer.from([1,0,0,0,0,0,224])]);
    t.same(protocol.encodeResponse(Number.MAX_VALUE), [Buffer.from([255,255,255,255,255,255,239,127])]);
    t.same(protocol.encodeResponse([Number.MAX_VALUE]), [Buffer.from([255,255,255,255,255,255,239,127])]);
    t.same(protocol.encodeResponse(Number.MIN_VALUE), [Buffer.from([1,0,0,0,0,0,0,0])]);
    t.same(protocol.encodeResponse([Number.MIN_VALUE]), [Buffer.from([1,0,0,0,0,0,0,0])]);
    t.same(protocol.encodeResponse(Number.EPSILON), [Buffer.from([0,0,0,0,0,0,176,60])]);
    t.same(protocol.encodeResponse(Math.PI), [Buffer.from([24,45,68,84,251,33,9,64])]);
    t.same(protocol.encodeResponse([Math.E]), [Buffer.from([105,87,20,139,10,191,5,64])]);
    t.same(protocol.decodeResponse([]), [undefined]);
    t.same(protocol.decodeResponse([Buffer.from([0])]), [0]);
    t.same(protocol.decodeResponse([Buffer.from([0,0,0,0,0,0,0,0])]), [0]);
    t.same(protocol.decodeResponse([Buffer.from([105,87,20,139,10,191,5,64])]), [Math.E]);
    t.same(protocol.decodeResponse([Buffer.from([255,255,255,255,255,255,31])]), [Number.MAX_SAFE_INTEGER]);
    t.same(protocol.decodeResponse([Buffer.from([1,0,0,0,0,0,0,0])]), [Number.MIN_VALUE]);
    t.same(protocol.decodeResponse([Buffer.from([24,45,68,84,251,33,9,64])]), [Math.PI]);
    t.throws(() => protocol.decodeResponse([Buffer.alloc(0)]), new TypeError('decode frames error: number must not be null at frame 1'));
    t.end();
  });

  suite.test('one required argument schema', t => {
    t.throws(() => new FramesProtocol(['string'], ['number'], {required: 2}), new TypeError("encoder schema error: too much required arguments"));
    var protocol = new FramesProtocol(['string'], ['number'], {required: 1});
    t.throws(() => protocol.encodeRequest(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest([]), new Error("encode frames error: not enough arguments"));
    t.same(protocol.encodeRequest(''), [Buffer.alloc(0)]);
    t.same(protocol.encodeRequest('foo'), [Buffer.from('foo')]);
    var buf = Buffer.from('bar');
    t.same(protocol.encodeRequest(buf), [Buffer.from('bar')]);
    t.equal(protocol.encodeRequest(buf)[0], buf);
    t.same(protocol.encodeRequest(0), [Buffer.from('0')]);
    t.throws(() => protocol.decodeRequest([]), new Error("decode frames error: not enough frames"));
    t.same(protocol.decodeRequest([Buffer.alloc(0)]), ['']);
    t.same(protocol.decodeRequest([Buffer.from('foo')]), ['foo']);
    t.same(protocol.decodeRequest([Buffer.from('0')]), ['0']);
    t.throws(() => protocol.encodeResponse(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeResponse([]), new Error("encode frames error: not enough arguments"));
    t.same(protocol.encodeResponse(0), [Buffer.from([0])]);
    t.same(protocol.encodeResponse([0]), [Buffer.from([0])]);
    t.throws(() => protocol.encodeResponse('0'), new TypeError('value is not a number'));
    t.throws(() => protocol.encodeResponse(['0']), new TypeError('value is not a number'));
    var buf = Buffer.from('bar');
    t.same(protocol.encodeResponse(buf), [Buffer.from('bar')]);
    t.equal(protocol.encodeResponse(buf)[0], buf);
    t.same(protocol.encodeResponse(Number.MAX_SAFE_INTEGER), [Buffer.from([255,255,255,255,255,255,31])]);
    t.same(protocol.encodeResponse([Number.MAX_SAFE_INTEGER]), [Buffer.from([255,255,255,255,255,255,31])]);
    t.same(protocol.encodeResponse(Number.MIN_SAFE_INTEGER), [Buffer.from([1,0,0,0,0,0,224])]);
    t.same(protocol.encodeResponse([Number.MIN_SAFE_INTEGER]), [Buffer.from([1,0,0,0,0,0,224])]);
    t.same(protocol.encodeResponse(Number.MAX_VALUE), [Buffer.from([255,255,255,255,255,255,239,127])]);
    t.same(protocol.encodeResponse([Number.MAX_VALUE]), [Buffer.from([255,255,255,255,255,255,239,127])]);
    t.same(protocol.encodeResponse(Number.MIN_VALUE), [Buffer.from([1,0,0,0,0,0,0,0])]);
    t.same(protocol.encodeResponse([Number.MIN_VALUE]), [Buffer.from([1,0,0,0,0,0,0,0])]);
    t.same(protocol.encodeResponse(Number.EPSILON), [Buffer.from([0,0,0,0,0,0,176,60])]);
    t.same(protocol.encodeResponse(Math.PI), [Buffer.from([24,45,68,84,251,33,9,64])]);
    t.same(protocol.encodeResponse([Math.E]), [Buffer.from([105,87,20,139,10,191,5,64])]);
    t.throws(() => protocol.decodeResponse([]), new Error("decode frames error: not enough frames"));
    t.same(protocol.decodeResponse([Buffer.from([0])]), [0]);
    t.same(protocol.decodeResponse([Buffer.from([0,0,0,0,0,0,0,0])]), [0]);
    t.same(protocol.decodeResponse([Buffer.from([105,87,20,139,10,191,5,64])]), [Math.E]);
    t.same(protocol.decodeResponse([Buffer.from([255,255,255,255,255,255,31])]), [Number.MAX_SAFE_INTEGER]);
    t.same(protocol.decodeResponse([Buffer.from([1,0,0,0,0,0,0,0])]), [Number.MIN_VALUE]);
    t.same(protocol.decodeResponse([Buffer.from([24,45,68,84,251,33,9,64])]), [Math.PI]);
    t.throws(() => protocol.decodeResponse([Buffer.alloc(0)]), new TypeError('decode frames error: number must not be null at frame 1'));
    t.end();
  });

  suite.test('many argument schema', t => {
    var protocol = new FramesProtocol(['string','int','bool','uint'], ['buffer','object'], {required: [3,1]});
    t.throws(() => protocol.encodeRequest(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest([]), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest('foo'), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest(['foo']), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest(['foo',1]), new Error("encode frames error: not enough arguments"));
    t.same(protocol.encodeRequest(['foo',-42,true]), [Buffer.from('foo'), Buffer.from([-42]), Buffer.from([1])]);
    t.same(protocol.encodeRequest(['bar',255,false,255]), [Buffer.from('bar'), Buffer.from([255,0]), Buffer.alloc(0), Buffer.from([255])]);
    var buf = Buffer.from('baz');
    t.same(protocol.encodeRequest([buf,buf,buf,buf]), [Buffer.from('baz'),Buffer.from('baz'),Buffer.from('baz'),Buffer.from('baz')]);
    t.equal(protocol.encodeRequest([buf,buf,buf])[0], buf);
    t.equal(protocol.encodeRequest([buf,buf,buf])[1], buf);
    t.equal(protocol.encodeRequest([buf,buf,buf])[2], buf);
    t.equal(protocol.encodeRequest([buf,buf,buf,buf])[3], buf);

    t.throws(() => protocol.encodeResponse(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeResponse([]), new Error("encode frames error: not enough arguments"));
    t.same(protocol.encodeResponse([buf]), [Buffer.from('baz')]);
    t.same(protocol.encodeResponse(buf), [Buffer.from('baz')]);
    t.equal(protocol.encodeResponse(buf)[0], buf);
    t.equal(protocol.encodeResponse([buf,buf])[0], buf);
    t.equal(protocol.encodeResponse([buf,buf])[1], buf);
    t.same(protocol.encodeResponse([buf, {moo: 'bee'}]), [Buffer.from('baz'), Buffer.from([129,163,109,111,111,163,98,101,101])]);
    t.throws(() => protocol.decodeResponse([]), new Error("decode frames error: not enough frames"));
    t.same(protocol.decodeResponse([buf]), [Buffer.from('baz'), undefined]);
    t.equal(protocol.decodeResponse([buf])[0], buf);
    t.same(protocol.decodeResponse([buf, Buffer.from([129,163,98,101,101,163,109,111,111])]), [Buffer.from('baz'), {bee: 'moo'}]);
    t.end();
  });

  suite.test('many argument schema extraArgs', t => {
    var protocol = new FramesProtocol(['string','int','bool','uint'], ['buffer','object'], {required: [3,1], extraArgs: [false, true]});
    t.throws(() => protocol.encodeRequest(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest([]), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest('foo'), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest(['foo']), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest(['foo',1]), new Error("encode frames error: not enough arguments"));
    t.same(protocol.encodeRequest(['foo',-42,true]), [Buffer.from('foo'), Buffer.from([-42]), Buffer.from([1])]);
    t.same(protocol.encodeRequest(['bar',255,false,255,'extra']), [Buffer.from('bar'), Buffer.from([255,0]), Buffer.alloc(0), Buffer.from([255])]);
    var buf = Buffer.from('baz');
    t.same(protocol.encodeRequest([buf,buf,buf,buf,'extra']), [Buffer.from('baz'),Buffer.from('baz'),Buffer.from('baz'),Buffer.from('baz')]);
    t.equal(protocol.encodeRequest([buf,buf,buf])[0], buf);
    t.equal(protocol.encodeRequest([buf,buf,buf])[1], buf);
    t.equal(protocol.encodeRequest([buf,buf,buf])[2], buf);
    t.equal(protocol.encodeRequest([buf,buf,buf,buf])[3], buf);

    t.throws(() => protocol.encodeResponse(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeResponse([]), new Error("encode frames error: not enough arguments"));
    t.same(protocol.encodeResponse([buf]), [Buffer.from('baz')]);
    t.same(protocol.encodeResponse([buf, undefined]), [Buffer.from('baz')]);
    t.same(protocol.encodeResponse(buf), [Buffer.from('baz')]);
    t.equal(protocol.encodeResponse(buf)[0], buf);
    t.equal(protocol.encodeResponse([buf,buf])[0], buf);
    t.equal(protocol.encodeResponse([buf,buf])[1], buf);
    t.same(protocol.encodeResponse([buf, {moo: 'bee'}]), [Buffer.from('baz'), Buffer.from([129,163,109,111,111,163,98,101,101])]);
    t.same(protocol.encodeResponse([buf, {moo: 'bee'}, 'extra']), [Buffer.from('baz'), Buffer.from([129,163,109,111,111,163,98,101,101]), 'extra']);
    t.same(protocol.encodeResponse([buf, {moo: 'bee'}, 'extra2', 'extra']), [Buffer.from('baz'), Buffer.from([129,163,109,111,111,163,98,101,101]), 'extra2', 'extra']);
    t.throws(() => protocol.decodeResponse([]), new Error("decode frames error: not enough frames"));
    t.same(protocol.decodeResponse([buf]), [Buffer.from('baz'), undefined]);
    t.equal(protocol.decodeResponse([buf])[0], buf);
    t.same(protocol.decodeResponse([buf, Buffer.from([129,163,98,101,101,163,109,111,111])]), [Buffer.from('baz'), {bee: 'moo'}]);
    t.same(protocol.decodeResponse([buf, Buffer.from([129,163,98,101,101,163,109,111,111]), 'extra']), [Buffer.from('baz'), {bee: 'moo'}, 'extra']);
    t.same(protocol.decodeResponse([buf, Buffer.from([129,163,98,101,101,163,109,111,111]), 'extra2', 'extra']), [Buffer.from('baz'), {bee: 'moo'}, 'extra2', 'extra']);
    t.end();
  });

  suite.test('rpc', t => {
    t.plan(26);
    var protocol = new FramesProtocol(['string','int','bool','uint'], ['buffer','object','hex'], {required: [3,1], extraArgs: [true, false]});
    var [router, url] = createZmqSocket('router');
    var listener, socket = new ZmqRpcSocket(url);
    var request = protocol.createRequestFunctionFor(socket);

    t.equal(socket.pending, null);
    return Promise.all([
      new Promise((resolve, reject) => {
        listener = protocol.createRouterMessageListener(router, (reply, args) => {
          try {
            t.type(reply, 'function');
            t.type(reply.requestId, Buffer);
            t.type(args, Array);
            t.equal(args.length, 5);
            t.equal(args[0], "foo");
            t.equal(args[1], -776);
            t.equal(args[2], true);
            t.equal(args[3], 1234567890);
            t.type(args[4], Buffer);
            t.same(args[4], Buffer.from('bar'));
            reply([Buffer.from('baz'), {cat: "meow", "ary": [1,2,3]},'deadbaca',Buffer.from('lost')]);
            resolve();
          } catch(e) { reject(e); }
        });
        t.type(listener, 'function');
      }),
      Promise.resolve().then(() => {
        t.equal(socket.pending, null);
        var promise = request(["foo",-776,true,1234567890,Buffer.from('bar')]);
        t.type(socket.pending, Promise);
        t.not(socket.pending, promise);
        t.throws(() => request("moo"), new Error("encode frames error: not enough arguments"));
        request(["moo",-1,false,0]).catch(err => {
          t.type(err, Error);
          t.equal(err.isCancel, undefined);
          t.equal(err.message, "ZmqRpcSocket: another request pending");
        });
        return promise.then(res => {
          t.type(res, Array);
          t.type(res.length, 3);
          t.type(res[0], Buffer);
          t.same(res[0], Buffer.from('baz'));
          t.same(res[1], {cat: "meow", "ary": [1,2,3]});
          t.equal(res[2], 'deadbaca');
        })
      })
      .then(() => {
        return new Promise((resolve, reject) => {
          socket.close();
          router.removeListener('frames', listener);
          router.unbind(url, err => {
            if (err) return reject(err);
            router.close();
            resolve();
          });
        });
      })
      .then(() => t.ok(true))
    ]).catch(t.threw);
  });

  suite.end();
});

function createZmqSocket(type, url) {
  var sock = new ZmqSocket(type);
  sock.setsockopt(ZMQ_LINGER, 0);
  do {
    url || (url = 'tcp://127.0.0.1:' + ((Math.random()*20000 + 10000) >>> 0));
    try {
      sock.bindSync(url);
    } catch(err) {
      url = undefined
    }
  } while(url === undefined);
  sock.unref();
  return [sock, url];
}
