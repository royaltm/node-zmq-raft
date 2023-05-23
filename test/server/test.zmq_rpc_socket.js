/*
 *  Copyright (c) 2016-2023 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const crypto = require('crypto');
const test = require('tap').test;
const raft = require('../..');
const { delay } = raft.utils.helpers;
const { ZmqRpcSocket, RpcCancelError } = raft.server.ZmqRpcSocket;
const { ZmqSocket } = raft.utils.zmqsocket;
const { ZMQ_LINGER } = require('zeromq');

test('should be a function', t => {
  t.type(ZmqRpcSocket, 'function');
  t.end();
});

test('router', suite => {

  suite.test('test requests', t => {
    t.plan(19);
    var [router, url] = createZmqSocket('router');
    var socket = new ZmqRpcSocket(url);
    t.equal(socket.pending, null);
    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('frames', (frames) => {
          try {
            let [src, id, msg] = frames;
            t.type(src, Buffer);
            t.type(id, Buffer);
            t.type(msg, Buffer);
            t.not(src.length, 0);
            t.not(id.length, 0);
            t.equal(msg.toString(), "foo");
            router.send([src, id, "foo", "bar"]);
            router.send([src, id, "ala"]);
            router.send([src, crypto.randomBytes(10), "kota"]);
            router.send([src, id, "ma"]);
            resolve();
          } catch(e) { reject(e); }
        })
      }),
      Promise.resolve().then(() => {
        t.equal(socket.pending, null);
        var promise = socket.request("foo");
        t.equal(socket.pending, promise);
        socket.request("moo").catch(err => {
          t.type(err, Error);
          t.equal(err.isCancel, undefined);
          t.equal(err.message, "ZmqRpcSocket: another request pending");
        });
        return promise.then(res => {
          t.type(res, Array);
          t.type(res.length, 2);
          t.type(res[0], Buffer);
          t.type(res[1], Buffer);
          t.equal(res[0].toString(), "foo");
          t.equal(res[1].toString(), "bar");
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
    t.equal(socket.pending, null);
    t.equal(socket.reset(), socket);
    var start = Date.now();
    return Promise.all([
      socket.request("foo").catch(err => {
        t.type(err, RpcCancelError);
        t.equal(err.isCancel, true);
        t.equal(err.message, "request cancelled");
        t.ok(Date.now() - start > 100);
      })
      .then(() => {
        socket.close();
        t.ok(true);
      }),
      delay(100).then(() => {
        t.type(socket.pending, Promise);
        t.equal(socket.reset(), socket);
        t.equal(socket.pending, null);
      })
    ]).catch(t.threw);
  });

  suite.test('test request after reset', t => {
    t.plan(18);
    var [router, url] = createZmqSocket('router');
    router.unbindSync(url);
    router.close();
    var socket = new ZmqRpcSocket(url, {sockopts: {ZMQ_RECONNECT_IVL: 10}});
    var start = Date.now();
    return Promise.all([
      socket.request("foo").catch(err => {
        t.type(err, RpcCancelError);
        t.equal(err.isCancel, true);
        t.equal(err.message, "request cancelled");
        t.ok(Date.now() - start >= 100);
      }),
      delay(100).then(() => {
        t.type(socket.pending, Promise);
        var promise = socket.reset().request("bar");
        t.equal(socket.pending, promise);
        return Promise.all([
          promise.then(res => {
            t.type(res, Array);
            t.type(res.length, 1);
            t.type(res[0], Buffer);
            t.equal(res[0].toString(), "baz");
          }),
          delay(500).then(() => new Promise((resolve, reject) => {
            var [router2, url2] = createZmqSocket('router', url);
            t.equal(url, url2);
            router = router2;
            var ignoreTimes = 3;
            router.on('frames', (frames) => {
              if (ignoreTimes-- > 0) return;
              try {
                let [src, id, msg] = frames;
                t.type(src, Buffer);
                t.type(id, Buffer);
                t.type(msg, Buffer);
                t.not(src.length, 0);
                t.not(id.length, 0);
                t.equal(msg.toString(), "bar");
                router.send([src, id, "baz"]);
                resolve();
              } catch(e) { reject(e); }
            })
          }))
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
    t.equal(socket.timeoutMs, 75);
    var start = Date.now();
    var numrequests = 0;
    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('frames', (frames) => {
          try {
            let [src, id, msg] = frames;
            t.type(src, Buffer);
            t.type(id, Buffer);
            t.type(msg, Buffer);
            t.not(src.length, 0);
            t.not(id.length, 0);
            t.equal(msg.toString(), "foo");
            t.type(socket.pending, Promise);
            if (++numrequests == 2) {
              t.equal(socket.reset(), socket);
              t.equal(socket.pending, null);
              resolve();
            }
          } catch(e) { reject(e); }
        })
      }),
      socket.request("foo").catch(err => {
        t.type(err, RpcCancelError);
        t.equal(err.isCancel, true);
        t.equal(err.message, "request cancelled");
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
