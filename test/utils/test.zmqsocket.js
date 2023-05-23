/*
 *  Copyright (c) 2016-2023 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const crypto = require('crypto');
const test = require('tap').test;
const raft = require('../..');
const delay = raft.utils.helpers.delay;
const { allocBufUIntLE, readBufUIntLE } = raft.utils.bufconv;
const { ZmqSocket, ZmqDealerSocket } = raft.utils.zmqsocket;
const zmq = require('zeromq');

test('should be a function', t => {
  t.type(ZmqSocket, 'function');
  t.type(ZmqDealerSocket, 'function');
  t.end();
});

test('router', suite => {

  suite.test('test requests', t => {
    t.plan(22);
    var [router, url] = createZmqSocket('router');
    var socket = createZmqDealerSocket(url);
    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('frames', (args) => {
          try {
            t.type(args, Array);
            let [src, msg] = args;
            t.type(src, Buffer);
            t.type(msg, Buffer);
            t.not(src.length, 0);
            t.equal(msg.toString(), "foo");
            router.send([src, "foo", "bar"]);
            resolve();
          } catch(e) { reject(e); }
        })
      }),
      new Promise((resolve, reject) => {
        t.equal(socket.send("foo"), false);
        t.equal(socket._zmq.pending, true);
        socket.connect(url);
        socket.on('drain', () => {
          try {
            t.equal(socket.send("foo"), true);
            t.equal(socket._zmq.pending, false);
          } catch(err) { reject(err); }
        });
        socket.on('frames', (frames) => {
          try {
            t.type(frames, Array);
            t.type(frames.length, 2);
            t.type(frames[0], Buffer);
            t.type(frames[1], Buffer);
            t.equal(frames[0].toString(), "foo");
            t.equal(frames[1].toString(), "bar");
            resolve();
          } catch(err) { reject(err); }
        });
      })
      .then(() => {
        router.unbindSync(url);
        router.close();
        return delay(100).then(() => {
          t.equal(socket.send("bar"), true);
          t.equal(socket._zmq.pending, false);
          t.equal(socket.send("baz"), false);
          t.equal(socket._zmq.pending, true);
          t.equal(socket.cancelSend(), socket);
          t.equal(socket._zmq.pending, false);
          socket.disconnect(url);
          socket.close();
        });
      }, err => {
        socket.close();
        router.unbindSync(url);
        router.close();
        throw err;
      })
      .then(() => t.ok(true))
    ]).catch(t.threw);
  });

  suite.test('test flood', t => {
    const total = 1000;
    t.plan(14*(total+1)+2+11*total+1);
    var [router, url] = createZmqSocket('router');
    var socket = createZmqDealerSocket(url);
    socket.connect(url);
    var reqcount = 0, sentmap = new Map([[0,0]]);
    return Promise.all([
      new Promise((resolve, reject) => {
        router.on('frames', (args) => {
          try {
            t.type(args, Array);
            let [src, msg1, msg2, msg3, msg4, msg5] = args;
            t.type(src, Buffer);
            t.type(msg1, Buffer);
            t.type(msg2, Buffer);
            t.type(msg3, Buffer);
            t.type(msg4, Buffer);
            t.type(msg5, Buffer);
            t.not(src.length, 0);
            t.equal(msg1.toString(), "ala");
            t.equal(msg2.toString(), "ma");
            t.equal(msg3.toString(), "kota");
            let x = readBufUIntLE(msg4);
            t.type(x, 'number');
            t.equal(sentmap.get(x), readBufUIntLE(msg5));
            sentmap.delete(x);
            t.equal(x, reqcount);
            if (reqcount++ === 0) {
              asyncTimes(total, (n) => {
                let len = Math.random()*100000>>>0;
                sentmap.set(++n, len);
                router.send([src, 'a','kot','ma', allocBufUIntLE(n), crypto.randomBytes(len)])
              });
            }
            if (reqcount === total + 1) resolve();
          } catch(e) { reject(e); }
        })
      }),
      new Promise((resolve, reject) => {
        t.equal(socket.send(["ala","ma",Buffer.from("kota"),allocBufUIntLE(0),allocBufUIntLE(0)]), true);
        t.equal(socket._zmq.pending, false);
        var slab = crypto.randomBytes(150000);
        var busy = false, queue = [];
        var send = () => {
          while (!busy && queue.length !== 0) {
            busy = true;
            let payload = queue.shift();
            if (socket.send(payload)) {
              busy = false;
            }
            else {
              queue.unshift(payload);
              socket.once('drain', () => {
                busy = false;
                send();
              });
            }
          }
        };
        socket.on('frames', (frames) => {
          try {
            t.type(frames, Array);
            t.type(frames.length, 5);
            t.type(frames[0], Buffer);
            t.type(frames[1], Buffer);
            t.type(frames[2], Buffer);
            t.type(frames[3], Buffer);
            t.type(frames[4], Buffer);
            t.equal(frames[0].toString(), "a");
            t.equal(frames[1].toString(), "kot");
            t.equal(frames[2].toString(), "ma");
            var x = readBufUIntLE(frames[3]);
            t.type(x, 'number');
            queue.push(["ala","ma",Buffer.from("kota"),allocBufUIntLE(x),allocBufUIntLE(frames[4].length),slab]);
            send();
            if (x === total) resolve();
          } catch(err) { reject(err); }
        });
      })
    ])
    .then(() => {
      router.unbindSync(url);
      router.close();
      socket.close();
    }, err => {
      socket.close();
      router.unbindSync(url);
      router.close();
      throw err;
    })
    .then(() => t.ok(true))
    .catch(t.threw);
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

function createZmqDealerSocket(url) {
  var socket = new ZmqDealerSocket();
  socket.setsockopt(zmq.ZMQ_LINGER, 0);
  socket.setsockopt(zmq.ZMQ_SNDHWM, 1);
  return socket;
}

function asyncTimes(n, cb) {
  var index = 0;
  return new Promise((resolve, reject) => {
    var cycle = () => setImmediate(() => {
      try {
        if (--n === 0) {
          resolve(cb(index));
        }
        else {
          cb(index++);
          cycle();
        }
      } catch(err) {
        reject(err);
      }
    });
    cycle();
  });
}
