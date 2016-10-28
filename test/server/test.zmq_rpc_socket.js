/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const { ZmqRpcSocket, RpcCancelError } = raft.server.ZmqRpcSocket;
const zmq = require('zmq');

test('should be a function', t => {
  t.type(ZmqRpcSocket, 'function');
  t.end();
});

test('router', suite => {

  suite.test('test requests', t => {
    t.plan(19);
    var [router, url] = createZmqSocket('router');
    var socket = new ZmqRpcSocket(url);
    t.strictEquals(socket.pending, null);
    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('message', (src, id, msg) => {
          try {
            t.type(src, Buffer);
            t.type(id, Buffer);
            t.type(msg, Buffer);
            t.notStrictEquals(src.length, 0);
            t.notStrictEquals(id.length, 0);
            t.strictEquals(msg.toString(), "foo");
            router.send([src, id, "foo", "bar"]);
            resolve();
          } catch(e) { reject(e); }
        })
      }),
      Promise.resolve().then(() => {
        t.strictEquals(socket.pending, null);
        var promise = socket.request("foo");
        t.strictEquals(socket.pending, promise);
        socket.request("moo").catch(err => {
          t.type(err, Error);
          t.strictEquals(err.isCancel, undefined);
          t.strictEquals(err.message, "ZmqRpcSocket: another request pending");
        });
        return promise.then(res => {
          t.type(res, Array);
          t.type(res.length, 2);
          t.type(res[0], Buffer);
          t.type(res[1], Buffer);
          t.strictEquals(res[0].toString(), "foo");
          t.strictEquals(res[1].toString(), "bar");
        })        
      })
      .then(() => {
        return new Promise((resolve, reject) => {
          socket.close();
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

  suite.test('test reset', t => {
    t.plan(10);
    var [router, url] = createZmqSocket('router');
    router.unbindSync(url);
    router.close();
    var socket = new ZmqRpcSocket(url);
    t.strictEquals(socket.pending, null);
    t.strictEquals(socket.reset(), socket);
    var start = Date.now();
    return Promise.all([
      socket.request("foo").catch(err => {
        t.type(err, RpcCancelError);
        t.strictEquals(err.isCancel, true);
        t.strictEquals(err.message, "request cancelled");
        t.ok(Date.now() - start > 100);
      })
      .then(() => {
        socket.close();
        t.ok(true);
      }),
      new Promise((resolve, reject) => {
        setTimeout(() => {
          try {
            t.type(socket.pending, Promise);
            t.strictEquals(socket.reset(), socket);
            t.strictEquals(socket.pending, null);
            resolve();
          } catch(e) { reject(e); }
        }, 100);
      })
    ]).catch(t.threw);
  });

  suite.test('test request after reset', t => {
    t.plan(18);
    var [router, url] = createZmqSocket('router');
    router.unbindSync(url);
    router.close();
    var socket = new ZmqRpcSocket(url);
    var start = Date.now();
    return Promise.all([
      socket.request("foo").catch(err => {
        t.type(err, RpcCancelError);
        t.strictEquals(err.isCancel, true);
        t.strictEquals(err.message, "request cancelled");
        t.ok(Date.now() - start >= 100);
      }),
      new Promise((resolve, reject) => setTimeout(resolve, 100))
      .then(() => {
        t.type(socket.pending, Promise);
        var promise = socket.reset().request("bar");
        t.strictEquals(socket.pending, promise);
        return Promise.all([
          promise.then(res => {
            t.type(res, Array);
            t.type(res.length, 1);
            t.type(res[0], Buffer);
            t.strictEquals(res[0].toString(), "baz");
          }),
          new Promise((resolve, reject) => {
            var [router2, url2] = createZmqSocket('router', url);
            t.strictEquals(url, url2);
            router = router2;
            router.on('message', (src, id, msg) => {
              try {
                t.type(src, Buffer);
                t.type(id, Buffer);
                t.type(msg, Buffer);
                t.notStrictEquals(src.length, 0);
                t.notStrictEquals(id.length, 0);
                t.strictEquals(msg.toString(), "bar");
                router.send([src, id, "baz"]);
                resolve();
              } catch(e) { reject(e); }
            })
          })
        ]);
      })
      .then(() => {
        return new Promise((resolve, reject) => {
          socket.close();
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

  suite.test('test reset after re-sending timed out request', t => {
    t.plan(1+7*2+2+4+1);
    var [router, url] = createZmqSocket('router');
    var socket = new ZmqRpcSocket(url, {timeout: 75});
    t.strictEquals(socket.timeoutMs, 75);
    var start = Date.now();
    var numrequests = 0;
    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('message', (src, id, msg) => {
          try {
            t.type(src, Buffer);
            t.type(id, Buffer);
            t.type(msg, Buffer);
            t.notStrictEquals(src.length, 0);
            t.notStrictEquals(id.length, 0);
            t.strictEquals(msg.toString(), "foo");
            t.type(socket.pending, Promise);
            if (++numrequests == 2) {
              t.strictEquals(socket.reset(), socket);
              t.strictEquals(socket.pending, null);
              resolve();
            }
          } catch(e) { reject(e); }
        })
      }),
      socket.request("foo").catch(err => {
        t.type(err, RpcCancelError);
        t.strictEquals(err.isCancel, true);
        t.strictEquals(err.message, "request cancelled");
        t.ok(Date.now() - start >= 75);
      })
      .then(() => {
        return new Promise((resolve, reject) => {
          socket.close();
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
  var sock = zmq.socket(type);
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
