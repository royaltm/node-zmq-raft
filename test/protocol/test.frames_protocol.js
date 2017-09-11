/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const FramesProtocol = raft.protocol.FramesProtocol;
const { ZmqRpcSocket, RpcCancelError } = raft.server.ZmqRpcSocket;
const { ZmqSocket } = raft.utils.zmqsocket;
const zmq = require('zmq');

test('should be a function', t => {
  t.type(FramesProtocol, 'function');
  t.end();
});

test('FramesProtocol', suite => {

  suite.test('empty schema', t => {
    var protocol = new FramesProtocol([], []);
    t.deepEquals(protocol.encodeRequest(), []);
    t.deepEquals(protocol.encodeRequest(''), []);
    t.deepEquals(protocol.encodeRequest([]), []);
    t.deepEquals(protocol.encodeRequest(['']), []);
    t.deepEquals(protocol.decodeRequest([]), []);
    t.deepEquals(protocol.decodeRequest(['']), []);
    t.deepEquals(protocol.encodeResponse(), []);
    t.deepEquals(protocol.encodeResponse(''), []);
    t.deepEquals(protocol.encodeResponse([]), []);
    t.deepEquals(protocol.encodeResponse(['']), []);
    t.deepEquals(protocol.decodeResponse([]), []);
    t.deepEquals(protocol.decodeResponse(['']), []);

    var protocol = new FramesProtocol([], [], {extraArgs: true});
    t.deepEquals(protocol.encodeRequest(), []);
    t.deepEquals(protocol.encodeRequest(''), ['']);
    t.deepEquals(protocol.encodeRequest([]), []);
    t.deepEquals(protocol.encodeRequest(['']), ['']);
    t.deepEquals(protocol.decodeRequest([]), []);
    t.deepEquals(protocol.decodeRequest(['']), ['']);
    t.deepEquals(protocol.encodeResponse(), []);
    t.deepEquals(protocol.encodeResponse(''), ['']);
    t.deepEquals(protocol.encodeResponse([]), []);
    t.deepEquals(protocol.encodeResponse(['']), ['']);
    t.deepEquals(protocol.decodeResponse([]), []);
    t.deepEquals(protocol.decodeResponse(['']), ['']);
    t.end();
  });

  suite.test('one argument schema', t => {
    var protocol = new FramesProtocol(['string'], ['number']);
    t.deepEquals(protocol.encodeRequest(), []);
    t.deepEquals(protocol.encodeRequest([]), []);
    t.deepEquals(protocol.encodeRequest(''), [Buffer.alloc(0)]);
    t.deepEquals(protocol.encodeRequest('foo'), [Buffer.from('foo')]);
    var buf = Buffer.from('bar');
    t.deepEquals(protocol.encodeRequest(buf), [Buffer.from('bar')]);
    t.strictEquals(protocol.encodeRequest(buf)[0], buf);
    t.deepEquals(protocol.encodeRequest(0), [Buffer.from('0')]);
    t.deepEquals(protocol.decodeRequest([]), [undefined]);
    t.deepEquals(protocol.decodeRequest([Buffer.alloc(0)]), ['']);
    t.deepEquals(protocol.decodeRequest([Buffer.from('foo')]), ['foo']);
    t.deepEquals(protocol.decodeRequest([Buffer.from('0')]), ['0']);
    t.deepEquals(protocol.encodeResponse(), []);
    t.deepEquals(protocol.encodeResponse([]), []);
    t.deepEquals(protocol.encodeResponse(0), [Buffer.from([0])]);
    t.deepEquals(protocol.encodeResponse([0]), [Buffer.from([0])]);
    t.throws(() => protocol.encodeResponse('0'), new TypeError('value is not a number'));
    t.throws(() => protocol.encodeResponse(['0']), new TypeError('value is not a number'));
    var buf = Buffer.from('bar');
    t.deepEquals(protocol.encodeResponse(buf), [Buffer.from('bar')]);
    t.strictEquals(protocol.encodeResponse(buf)[0], buf);
    t.deepEquals(protocol.encodeResponse(Number.MAX_SAFE_INTEGER), [Buffer.from([255,255,255,255,255,255,31])]);
    t.deepEquals(protocol.encodeResponse([Number.MAX_SAFE_INTEGER]), [Buffer.from([255,255,255,255,255,255,31])]);
    t.deepEquals(protocol.encodeResponse(Number.MIN_SAFE_INTEGER), [Buffer.from([1,0,0,0,0,0,224])]);
    t.deepEquals(protocol.encodeResponse([Number.MIN_SAFE_INTEGER]), [Buffer.from([1,0,0,0,0,0,224])]);
    t.deepEquals(protocol.encodeResponse(Number.MAX_VALUE), [Buffer.from([255,255,255,255,255,255,239,127])]);
    t.deepEquals(protocol.encodeResponse([Number.MAX_VALUE]), [Buffer.from([255,255,255,255,255,255,239,127])]);
    t.deepEquals(protocol.encodeResponse(Number.MIN_VALUE), [Buffer.from([1,0,0,0,0,0,0,0])]);
    t.deepEquals(protocol.encodeResponse([Number.MIN_VALUE]), [Buffer.from([1,0,0,0,0,0,0,0])]);
    t.deepEquals(protocol.encodeResponse(Number.EPSILON), [Buffer.from([0,0,0,0,0,0,176,60])]);
    t.deepEquals(protocol.encodeResponse(Math.PI), [Buffer.from([24,45,68,84,251,33,9,64])]);
    t.deepEquals(protocol.encodeResponse([Math.E]), [Buffer.from([105,87,20,139,10,191,5,64])]);
    t.deepEquals(protocol.decodeResponse([]), [undefined]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([0])]), [0]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([0,0,0,0,0,0,0,0])]), [0]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([105,87,20,139,10,191,5,64])]), [Math.E]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([255,255,255,255,255,255,31])]), [Number.MAX_SAFE_INTEGER]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([1,0,0,0,0,0,0,0])]), [Number.MIN_VALUE]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([24,45,68,84,251,33,9,64])]), [Math.PI]);
    t.throws(() => protocol.decodeResponse([Buffer.alloc(0)]), new TypeError('decode frames error: number must not be null at frame 1'));
    t.end();
  });

  suite.test('one required argument schema', t => {
    t.throws(() => new FramesProtocol(['string'], ['number'], {required: 2}), new TypeError("encoder schema error: too much required arguments"));
    var protocol = new FramesProtocol(['string'], ['number'], {required: 1});
    t.throws(() => protocol.encodeRequest(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest([]), new Error("encode frames error: not enough arguments"));
    t.deepEquals(protocol.encodeRequest(''), [Buffer.alloc(0)]);
    t.deepEquals(protocol.encodeRequest('foo'), [Buffer.from('foo')]);
    var buf = Buffer.from('bar');
    t.deepEquals(protocol.encodeRequest(buf), [Buffer.from('bar')]);
    t.strictEquals(protocol.encodeRequest(buf)[0], buf);
    t.deepEquals(protocol.encodeRequest(0), [Buffer.from('0')]);
    t.throws(() => protocol.decodeRequest([]), new Error("decode frames error: not enough frames"));
    t.deepEquals(protocol.decodeRequest([Buffer.alloc(0)]), ['']);
    t.deepEquals(protocol.decodeRequest([Buffer.from('foo')]), ['foo']);
    t.deepEquals(protocol.decodeRequest([Buffer.from('0')]), ['0']);
    t.throws(() => protocol.encodeResponse(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeResponse([]), new Error("encode frames error: not enough arguments"));
    t.deepEquals(protocol.encodeResponse(0), [Buffer.from([0])]);
    t.deepEquals(protocol.encodeResponse([0]), [Buffer.from([0])]);
    t.throws(() => protocol.encodeResponse('0'), new TypeError('value is not a number'));
    t.throws(() => protocol.encodeResponse(['0']), new TypeError('value is not a number'));
    var buf = Buffer.from('bar');
    t.deepEquals(protocol.encodeResponse(buf), [Buffer.from('bar')]);
    t.strictEquals(protocol.encodeResponse(buf)[0], buf);
    t.deepEquals(protocol.encodeResponse(Number.MAX_SAFE_INTEGER), [Buffer.from([255,255,255,255,255,255,31])]);
    t.deepEquals(protocol.encodeResponse([Number.MAX_SAFE_INTEGER]), [Buffer.from([255,255,255,255,255,255,31])]);
    t.deepEquals(protocol.encodeResponse(Number.MIN_SAFE_INTEGER), [Buffer.from([1,0,0,0,0,0,224])]);
    t.deepEquals(protocol.encodeResponse([Number.MIN_SAFE_INTEGER]), [Buffer.from([1,0,0,0,0,0,224])]);
    t.deepEquals(protocol.encodeResponse(Number.MAX_VALUE), [Buffer.from([255,255,255,255,255,255,239,127])]);
    t.deepEquals(protocol.encodeResponse([Number.MAX_VALUE]), [Buffer.from([255,255,255,255,255,255,239,127])]);
    t.deepEquals(protocol.encodeResponse(Number.MIN_VALUE), [Buffer.from([1,0,0,0,0,0,0,0])]);
    t.deepEquals(protocol.encodeResponse([Number.MIN_VALUE]), [Buffer.from([1,0,0,0,0,0,0,0])]);
    t.deepEquals(protocol.encodeResponse(Number.EPSILON), [Buffer.from([0,0,0,0,0,0,176,60])]);
    t.deepEquals(protocol.encodeResponse(Math.PI), [Buffer.from([24,45,68,84,251,33,9,64])]);
    t.deepEquals(protocol.encodeResponse([Math.E]), [Buffer.from([105,87,20,139,10,191,5,64])]);
    t.throws(() => protocol.decodeResponse([]), new Error("decode frames error: not enough frames"));
    t.deepEquals(protocol.decodeResponse([Buffer.from([0])]), [0]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([0,0,0,0,0,0,0,0])]), [0]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([105,87,20,139,10,191,5,64])]), [Math.E]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([255,255,255,255,255,255,31])]), [Number.MAX_SAFE_INTEGER]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([1,0,0,0,0,0,0,0])]), [Number.MIN_VALUE]);
    t.deepEquals(protocol.decodeResponse([Buffer.from([24,45,68,84,251,33,9,64])]), [Math.PI]);
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
    t.deepEquals(protocol.encodeRequest(['foo',-42,true]), [Buffer.from('foo'), Buffer.from([-42]), Buffer.from([1])]);
    t.deepEquals(protocol.encodeRequest(['bar',255,false,255]), [Buffer.from('bar'), Buffer.from([255,0]), Buffer.alloc(0), Buffer.from([255])]);
    var buf = Buffer.from('baz');
    t.deepEquals(protocol.encodeRequest([buf,buf,buf,buf]), [Buffer.from('baz'),Buffer.from('baz'),Buffer.from('baz'),Buffer.from('baz')]);
    t.strictEquals(protocol.encodeRequest([buf,buf,buf])[0], buf);
    t.strictEquals(protocol.encodeRequest([buf,buf,buf])[1], buf);
    t.strictEquals(protocol.encodeRequest([buf,buf,buf])[2], buf);
    t.strictEquals(protocol.encodeRequest([buf,buf,buf,buf])[3], buf);

    t.throws(() => protocol.encodeResponse(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeResponse([]), new Error("encode frames error: not enough arguments"));
    t.deepEquals(protocol.encodeResponse([buf]), [Buffer.from('baz')]);
    t.deepEquals(protocol.encodeResponse(buf), [Buffer.from('baz')]);
    t.strictEquals(protocol.encodeResponse(buf)[0], buf);
    t.strictEquals(protocol.encodeResponse([buf,buf])[0], buf);
    t.strictEquals(protocol.encodeResponse([buf,buf])[1], buf);
    t.deepEquals(protocol.encodeResponse([buf, {moo: 'bee'}]), [Buffer.from('baz'), Buffer.from([129,163,109,111,111,163,98,101,101])]);
    t.throws(() => protocol.decodeResponse([]), new Error("decode frames error: not enough frames"));
    t.deepEquals(protocol.decodeResponse([buf]), [Buffer.from('baz'), undefined]);
    t.strictEquals(protocol.decodeResponse([buf])[0], buf);
    t.deepEquals(protocol.decodeResponse([buf, Buffer.from([129,163,98,101,101,163,109,111,111])]), [Buffer.from('baz'), {bee: 'moo'}]);
    t.end();
  });

  suite.test('many argument schema extraArgs', t => {
    var protocol = new FramesProtocol(['string','int','bool','uint'], ['buffer','object'], {required: [3,1], extraArgs: [false, true]});
    t.throws(() => protocol.encodeRequest(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest([]), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest('foo'), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest(['foo']), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeRequest(['foo',1]), new Error("encode frames error: not enough arguments"));
    t.deepEquals(protocol.encodeRequest(['foo',-42,true]), [Buffer.from('foo'), Buffer.from([-42]), Buffer.from([1])]);
    t.deepEquals(protocol.encodeRequest(['bar',255,false,255,'extra']), [Buffer.from('bar'), Buffer.from([255,0]), Buffer.alloc(0), Buffer.from([255])]);
    var buf = Buffer.from('baz');
    t.deepEquals(protocol.encodeRequest([buf,buf,buf,buf,'extra']), [Buffer.from('baz'),Buffer.from('baz'),Buffer.from('baz'),Buffer.from('baz')]);
    t.strictEquals(protocol.encodeRequest([buf,buf,buf])[0], buf);
    t.strictEquals(protocol.encodeRequest([buf,buf,buf])[1], buf);
    t.strictEquals(protocol.encodeRequest([buf,buf,buf])[2], buf);
    t.strictEquals(protocol.encodeRequest([buf,buf,buf,buf])[3], buf);

    t.throws(() => protocol.encodeResponse(), new Error("encode frames error: not enough arguments"));
    t.throws(() => protocol.encodeResponse([]), new Error("encode frames error: not enough arguments"));
    t.deepEquals(protocol.encodeResponse([buf]), [Buffer.from('baz')]);
    t.deepEquals(protocol.encodeResponse([buf, undefined]), [Buffer.from('baz')]);
    t.deepEquals(protocol.encodeResponse(buf), [Buffer.from('baz')]);
    t.strictEquals(protocol.encodeResponse(buf)[0], buf);
    t.strictEquals(protocol.encodeResponse([buf,buf])[0], buf);
    t.strictEquals(protocol.encodeResponse([buf,buf])[1], buf);
    t.deepEquals(protocol.encodeResponse([buf, {moo: 'bee'}]), [Buffer.from('baz'), Buffer.from([129,163,109,111,111,163,98,101,101])]);
    t.deepEquals(protocol.encodeResponse([buf, {moo: 'bee'}, 'extra']), [Buffer.from('baz'), Buffer.from([129,163,109,111,111,163,98,101,101]), 'extra']);
    t.deepEquals(protocol.encodeResponse([buf, {moo: 'bee'}, 'extra2', 'extra']), [Buffer.from('baz'), Buffer.from([129,163,109,111,111,163,98,101,101]), 'extra2', 'extra']);
    t.throws(() => protocol.decodeResponse([]), new Error("decode frames error: not enough frames"));
    t.deepEquals(protocol.decodeResponse([buf]), [Buffer.from('baz'), undefined]);
    t.strictEquals(protocol.decodeResponse([buf])[0], buf);
    t.deepEquals(protocol.decodeResponse([buf, Buffer.from([129,163,98,101,101,163,109,111,111])]), [Buffer.from('baz'), {bee: 'moo'}]);
    t.deepEquals(protocol.decodeResponse([buf, Buffer.from([129,163,98,101,101,163,109,111,111]), 'extra']), [Buffer.from('baz'), {bee: 'moo'}, 'extra']);
    t.deepEquals(protocol.decodeResponse([buf, Buffer.from([129,163,98,101,101,163,109,111,111]), 'extra2', 'extra']), [Buffer.from('baz'), {bee: 'moo'}, 'extra2', 'extra']);
    t.end();
  });

  suite.test('rpc', t => {
    t.plan(26);
    var protocol = new FramesProtocol(['string','int','bool','uint'], ['buffer','object','hex'], {required: [3,1], extraArgs: [true, false]});
    var [router, url] = createZmqSocket('router');
    var listener, socket = new ZmqRpcSocket(url);
    var request = protocol.createRequestFunctionFor(socket);

    t.strictEquals(socket.pending, null);
    return Promise.all([
      new Promise((resolve, reject) => {
        listener = protocol.createRouterMessageListener(router, (reply, args) => {
          try {
            t.type(reply, 'function');
            t.type(reply.requestId, Buffer);
            t.type(args, Array);
            t.strictEquals(args.length, 5);
            t.strictEquals(args[0], "foo");
            t.strictEquals(args[1], -776);
            t.strictEquals(args[2], true);
            t.strictEquals(args[3], 1234567890);
            t.type(args[4], Buffer);
            t.deepEquals(args[4], Buffer.from('bar'));
            reply([Buffer.from('baz'), {cat: "meow", "ary": [1,2,3]},'deadbaca',Buffer.from('lost')]);
            resolve();
          } catch(e) { reject(e); }
        });
        t.type(listener, 'function');
      }),
      Promise.resolve().then(() => {
        t.strictEquals(socket.pending, null);
        var promise = request(["foo",-776,true,1234567890,Buffer.from('bar')]);
        t.type(socket.pending, Promise);
        t.notStrictEquals(socket.pending, promise);
        t.throws(() => request("moo"), new Error("encode frames error: not enough arguments"));
        request(["moo",-1,false,0]).catch(err => {
          t.type(err, Error);
          t.strictEquals(err.isCancel, undefined);
          t.strictEquals(err.message, "ZmqRpcSocket: another request pending");
        });
        return promise.then(res => {
          t.type(res, Array);
          t.type(res.length, 3);
          t.type(res[0], Buffer);
          t.deepEquals(res[0], Buffer.from('baz'));
          t.deepEquals(res[1], {cat: "meow", "ary": [1,2,3]});
          t.strictEquals(res[2], 'deadbaca');
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
  sock.setsockopt(zmq.ZMQ_LINGER, 0);
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
