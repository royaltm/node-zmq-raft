/*
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const raft = require('../..');
const ZmqProtocolSocket = raft.client.ZmqProtocolSocket;
const FramesProtocol = raft.protocol.FramesProtocol;
const { ZmqSocket } = raft.utils.zmqsocket;
const { ZMQ_LINGER } = require('zeromq');

test('should be a function', t => {
  t.type(ZmqProtocolSocket, 'function');
  t.end();
});

test('ZmqProtocolSocket', suite => {
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
            t.equal(id.length, 1);
            t.type(msg, Buffer);
            t.not(src.length, 0);
            t.not(id.length, 0);
            t.equal(msg.toString(), "foo");
            router.send([src, id, "foo", "bar"]);
            resolve();
          } catch(e) { reject(e); }
        })
      }),
      socket.request("foo").then(res => {
        t.type(res, Array);
        t.equal(res.length, 2);
        t.type(res[0], Buffer);
        t.type(res[1], Buffer);
        t.equal(res[0].toString(), "foo");
        t.equal(res[1].toString(), "bar");
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
      t.equal(err.isTimeout, true);
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
            t.equal(id.length, 1);
            t.type(msg, Buffer);
            t.not(src.length, 0);
            t.not(id.length, 0);
            t.equal(msg.toString(), "foo");
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
            t.equal(res[0].toString(), "foo");
            t.equal(res[1].toString(), "bar");
            refresh();
          }
          else if (res.length === 1) {
            t.ok(Date.now() - start >= 100);
            t.type(res[0], Buffer);
            t.equal(res[0].toString(), "baz");
            refresh(250);
          }
          else if (res.length === 0) {
            t.ok(Date.now() - start >= 300);
            t.equal(res.length, 0);
            throw new Error("i am so bad");
          }
        }
      }).catch(err => {
        t.type(err, Error);
        t.equal(err.message, "i am so bad");
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
            t.equal(reply.requestId.toString('hex'), requestIdStr);
            t.type(args, Array);
            t.equal(args.length, 5);
            t.equal(args[0], "foo");
            t.equal(args[1], -776);
            t.type(args[2], 'boolean');
            t.equal(args[3], 1234567890);
            t.type(args[4], Buffer);
            t.same(args[4], Buffer.from('bar'));
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
            t.equal(res[0].length, 3);
            t.same(res[1], {cat: "meow", "ary": [1,2,3]});
            t.equal(res[2], 'deadbaca');
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
            t.equal(err.message, "encode frames error: not enough arguments");
          }),
          promise.then(res => {
            t.equal(res, 'OK');
          })
        ]);
      })
      .then(() => {
        socket.destroy();
        router.removeListener('frames', listener);
        router.unbindSync(url);
        router.close();
      })
      .then(() => t.ok(true))
    ]).catch(t.threw);
  });

  suite.test('waitForQueues', t => {
    t.plan(3+8*7+36);
    var [router, url] = createZmqSocket('router');
    router.unbindSync(url);
    var socket = new ZmqProtocolSocket(url, {highwatermark: 1});
    t.equal(socket.getsockopt('ZMQ_SNDHWM'), 1);
    t.equal(socket.pendingRequests, 0);
    t.equal(socket.pendingQueues, 0);
    var promise = Promise.all([
      new Promise((resolve, reject) => {
        var count = 8;
        router.on('frames', (frames) => {
          try {
            let [src, id, msg] = frames;
            t.type(src, Buffer);
            t.type(id, Buffer);
            t.equal(id.length, 1);
            t.type(msg, Buffer);
            t.not(src.length, 0);
            t.not(id.length, 0);
            t.match(msg.toString(), /^(foo|bar|baz|trigger|reply[1-4])$/);
            if (msg.toString() === 'baz') {
              router.send([src, id, 'more']);
            }
            else if (!msg.toString().startsWith('reply')) {
              router.send([src, id, "foo", "bar", "baz"]);
            }
            if (--count === 0) resolve();
          } catch(e) { reject(e); }
        })
      }),
      socket.request("foo").then(res => {
        t.type(res, Array);
        t.equal(res.length, 3);
        t.type(res[0], Buffer);
        t.type(res[1], Buffer);
        t.type(res[2], Buffer);
        t.equal(res[0].toString(), "foo");
        t.equal(res[1].toString(), "bar");
        t.equal(res[2].toString(), "baz");
      }),
      socket.request("bar").then(res => {
        t.type(res, Array);
        t.equal(res.length, 3);
        t.type(res[0], Buffer);
        t.type(res[1], Buffer);
        t.type(res[2], Buffer);
        t.equal(res[0].toString(), "foo");
        t.equal(res[1].toString(), "bar");
        t.equal(res[2].toString(), "baz");
      }),
      socket.request("baz", {onresponse: (res, reply, refresh) => {
        if (res[0].toString() === 'more') {
          reply('reply1');
          reply('trigger');
          refresh();
        }
        else {
          socket.setsockopt('ZMQ_SNDHWM', 2);
          reply('reply2');
          reply('reply3');
          reply('reply4');
          return res;
        }
      }}).then(res => {
        t.type(res, Array);
        t.equal(res.length, 3);
        t.type(res[0], Buffer);
        t.type(res[1], Buffer);
        t.type(res[2], Buffer);
        t.equal(res[0].toString(), "foo");
        t.equal(res[1].toString(), "bar");
        t.equal(res[2].toString(), "baz");
        t.equal(socket.pendingRequests, 0);
        t.equal(socket.pendingQueues, 1);
        return socket.waitForQueues(1000).then(() => {
          t.equal(socket.pendingQueues, 0);
        })
      }),
      socket.waitForQueues(1000).then(() => {
        t.not(socket.pendingRequests, 0);
        t.equal(socket.pendingQueues, 0);
      }),
      socket.waitForQueues(1).catch(err => {
        t.type(err, ZmqProtocolSocket.TimeoutError);
        t.equal(err.isTimeout, true);
        router.bindSync(url);
      })
    ])
    .then(() => {
      t.equal(socket.pendingRequests, 0);
      t.equal(socket.pendingQueues, 0);
      router.close();
      socket.destroy();
      t.ok(true);
    })

    t.equal(socket.pendingRequests, 3);
    t.equal(socket.pendingQueues, 2);
    return promise.catch(t.threw);
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
