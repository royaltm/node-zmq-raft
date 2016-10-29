/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const ZmqProtocolSocket = raft.client.ZmqProtocolSocket;
const FramesProtocol = raft.protocol.FramesProtocol;
const { ZmqSocket } = raft.utils.zmqsocket;
const { ZMQ_LINGER } = require('zmq');

test('should be a function', t => {
  t.type(ZmqProtocolSocket, 'function');
  t.end();
});

test('router', suite => {
  var [router, url] = createZmqSocket('router');

  suite.test('test requests', t => {
    t.plan(14);
    var socket = new ZmqProtocolSocket(url);
    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('frames', (frames) => {
          try {
            let [src, id, msg] = frames;
            t.type(src, Buffer);
            t.type(id, Buffer);
            t.strictEquals(id.length, 1);
            t.type(msg, Buffer);
            t.notStrictEquals(src.length, 0);
            t.notStrictEquals(id.length, 0);
            t.strictEquals(msg.toString(), "foo");
            router.send([src, id, "foo", "bar"]);
            resolve();
          } catch(e) { reject(e); }
        })
      }),
      socket.request("foo").then(res => {
        t.type(res, Array);
        t.strictEquals(res.length, 2);
        t.type(res[0], Buffer);
        t.type(res[1], Buffer);
        t.strictEquals(res[0].toString(), "foo");
        t.strictEquals(res[1].toString(), "bar");
        router.unbindSync(url);
      })
      .then(() => {
        router.close();
        socket.close();
        t.ok(true);
      })
    ]).catch(t.threw);
  });

  suite.test('test request timeout', t => {
    t.plan(4);
    var socket = new ZmqProtocolSocket(url, {timeout: 100});
    var start = Date.now();
    socket.request("foo").catch(err => {
      t.type(err, ZmqProtocolSocket.TimeoutError);
      t.strictEquals(err.isTimeout, true);
      t.ok(Date.now() - start >= 100);
    })
    .then(() => {
      socket.close();
      t.ok(true);
    })
    .catch(t.threw);
  });

  suite.test('test requests onresponse', t => {
    var [router, url] = createZmqSocket('router');
    t.plan(19+3*3+1);
    var socket = new ZmqProtocolSocket(url);
    var listener;
    var start = Date.now();
    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('frames', listener = (frames) => {
          try {
            let [src, id, msg] = frames;
            t.type(src, Buffer);
            t.type(id, Buffer);
            t.strictEquals(id.length, 1);
            t.type(msg, Buffer);
            t.notStrictEquals(src.length, 0);
            t.notStrictEquals(id.length, 0);
            t.strictEquals(msg.toString(), "foo");
            setTimeout(() => router.send([src, id, "foo", "bar"]), 50);
            setTimeout(() => router.send([src, id, "baz"]), 100);
            setTimeout(() => router.send([src, id]), 300);
            resolve();
          } catch(e) { reject(e); }
        })
      }),
      socket.request("foo", {
        timeout: 100,
        onresponse: (res, reply, refresh) => {
          t.type(res, Array);
          t.type(reply, 'function');
          t.type(refresh, 'function');
          if (res.length === 2) {
            t.ok(Date.now() - start >= 50);
            t.type(res[0], Buffer);
            t.type(res[1], Buffer);
            t.strictEquals(res[0].toString(), "foo");
            t.strictEquals(res[1].toString(), "bar");            
            refresh();
          }
          else if (res.length === 1) {
            t.ok(Date.now() - start >= 100);
            t.type(res[0], Buffer);
            t.strictEquals(res[0].toString(), "baz");
            refresh(250);
          }
          else if (res.length === 0) {
            t.ok(Date.now() - start >= 300);
            t.strictEquals(res.length, 0);
            throw new Error("i am so bad");
          }
        }
      }).catch(err => {
        t.type(err, Error);
        t.strictEquals(err.message, "i am so bad");
      })
    ])
    .then(() => {
      socket.close();
      router.removeListener('frames', listener);
      router.unbindSync(url);
      router.close();
      t.ok(true);
    })
    .catch(t.threw);
  });

  suite.test('protocol', t => {
    t.plan(11*2+1+2*6+4);
    var protocol = new FramesProtocol(['string','int','bool','uint'], ['buffer','object','hex'], {required: [3,1], extraArgs: [true, false]});
    var [router, url] = createZmqSocket('router');
    var listener, socket = new ZmqProtocolSocket(url, {protocol: protocol});
    var requestIdStr = 'deadbadbacabeefdeadbadba';

    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('error', err => reject(err));
        listener = protocol.createRouterMessageListener(router, (reply, args) => {
          try {
            t.type(reply, 'function');
            t.type(reply.requestId, Buffer);
            t.strictEquals(reply.requestId.toString('hex'), requestIdStr);
            t.type(args, Array);
            t.strictEquals(args.length, 5);
            t.strictEquals(args[0], "foo");
            t.strictEquals(args[1], -776);
            t.type(args[2], 'boolean');
            t.strictEquals(args[3], 1234567890);
            t.type(args[4], Buffer);
            t.deepEquals(args[4], Buffer.from('bar'));
            if (args[2] === true) {
              reply([Buffer.from('goo'), {cat: "meow", "ary": [1,2,3]},'deadbaca', Buffer.from('lost')]);
            }
            if (args[2] === false) {
              reply(['bar', {cat: "meow", "ary": [1,2,3]},'deadbaca', Buffer.from('lost')]);
            }
            resolve();
          } catch(e) { reject(e); }
        });
        t.type(listener, 'function');
      }),
      Promise.resolve().then(() => {
        var promise = socket.request(["foo",-776,true,1234567890,Buffer.from('bar')], {
          id: requestIdStr,
          onresponse: (res, reply, refresh) => {
            t.type(res, Array);
            t.type(res.length, 3);
            t.type(res[0], Buffer);
            t.strictEquals(res[0].length, 3);
            t.deepEquals(res[1], {cat: "meow", "ary": [1,2,3]});
            t.strictEquals(res[2], 'deadbaca');
            if (res[0].toString() === 'goo') {
              reply([Buffer.from("foo"),-776,false,1234567890,Buffer.from('bar')]);
            }
            else if (res[0].toString() === 'bar') {
              return 'OK';
            }
            else t.ok(false);
          }
        });
        return Promise.all([
          socket.request("moo").catch(err => {
            t.type(err, Error);
            t.strictEquals(err.message, "encode frames error: not enough arguments");
          }),
          promise.then(res => {
            t.strictEquals(res, 'OK');
          })
        ]);
      })
      .then(() => {
        socket.close();
        router.removeListener('frames', listener);
        router.unbindSync(url);
        router.close();
      })
      .then(() => t.ok(true))
    ]).catch(t.threw);
  });

  suite.end();
});

function createZmqSocket(type) {
  var url, sock = new ZmqSocket(type);
  sock.setsockopt(ZMQ_LINGER, 0);
  do {
    url = 'tcp://127.0.0.1:' + ((Math.random()*20000 + 10000) >>> 0);
    try {
      sock.bindSync(url);
    } catch(err) {
      url = undefined
    }
  } while(url === undefined);
  sock.unref();
  return [sock, url];
}
