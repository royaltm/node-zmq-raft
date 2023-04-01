/*
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const path = require('path');
const crypto = require('crypto');
const raft = require('../..');
const { delay } = raft.utils.helpers;
const { exclusive, shared } = raft.utils.lock;
const { open, close, read, write, unlink } = raft.utils.fsutil;
const tempfile = path.join(__dirname, '../../tmp/test.lock.file.' + process.pid);

test('should be a function', t => {
  t.type(shared, 'function');
  t.equal(shared.length, 2);
  t.type(exclusive, 'function');
  t.equal(exclusive.length, 2);
  t.end();
});

test('exclusive', t => {
  t.plan(16);
  var scope = {};
  t.type(exclusive(scope, () => t.ok(true)), Promise);
  var start = Date.now();
  return Promise.all([
    exclusive(scope, () => delay(100).then(() => {
      t.equal(scope.tap, undefined);
      return scope.tap = 1;
    })).then(x => {
      t.equal(x, 1);
    }),

    exclusive(scope, () => Promise.reject("baaa"))
    .catch(msg => {
      t.equal(msg, "baaa");
    }),

    exclusive(scope, () => new Promise((resolve, reject) => setImmediate(() => {
      t.equal(scope.tap, 1);
      scope.tap = 2;
      resolve(2);
    }))).then(x => {
      t.equal(x, 2);
    }),

    exclusive(scope, () => delay(10).then(() => {
      t.equal(scope.tap, 2);
      return scope.tap = 3;
    })).then(x => {
      t.equal(x, 3);
    }),

    exclusive(scope, () => {
      t.equal(scope.tap, 3);
      return scope.tap = 4;
    }).then(x => {
      t.equal(x, 4);
    }),

    exclusive(scope, () => {
      t.equal(scope.tap, 4);
      throw new Error("foo");
    }).catch(err => {
      t.type(err, Error);
      t.equal(err.message, "foo");
    }),

  ])
  .then(() => {
    var delta = Date.now() - start;
    t.equal(scope.tap, 4);
    t.ok(delta > 110, 'not ok, was: ' + delta);
  })
  .catch(t.threw);
});

test('shared', t => {
  t.plan(20);
  var scope = {};
  t.type(shared(scope, () => t.ok(true)), Promise);
  var start = Date.now();
  return Promise.all([
    shared(scope, () => {
      t.equal(scope.tap, undefined);
      return delay(20).then(() => {
        t.equal(scope.tap, 1);
        return scope.tap = 2;
      });
    }).then(x => {
      t.equal(x, 2);
    }),

    shared(scope, () => {
      return delay(10).then(() => {
        t.equal(scope.tap, undefined);
        return scope.tap = 1;
      });
    }).then(x => {
      t.equal(x, 1);
    }),

    exclusive(scope, () => {
      t.equal(scope.tap, 2);
      return delay(50).then(() => {
        t.equal(scope.tap, 2);
        return scope.tap = 3;
      })
    }).then(x => {
      t.equal(x, 3);
    }),

    shared(scope, () => {
      t.equal(scope.tap, 3);
      return delay(20).then(() => {
        t.equal(scope.tap, 4);
        return scope.tap = 5;
      });
    }).then(x => {
      t.equal(x, 5);
    }),

    shared(scope, () => delay(10).then(() => {
      t.equal(scope.tap, 3);
      return scope.tap = 4;
    })).then(x => {
      t.equal(x, 4);
    }),

    exclusive(scope, () => {
      t.equal(scope.tap, 5);
      return delay(10).then(() => {
        t.equal(scope.tap, 5);
        return scope.tap = 6;
      })
    }).then(x => {
      t.equal(x, 6);
    }),
  ])
  .then(() => {
    var delta = Date.now() - start;
    t.equal(scope.tap, 6);
    t.ok(delta > 100, 'not ok, was: ' + delta);
  })
  .catch(t.threw);
});

test('torture', t => {
  t.plan(1+2*10+1+2*10+2);
  var scope = {fd: null};
  var buffer = crypto.randomBytes(512*1024);
  var buffer2 = crypto.randomBytes(512*1024);
  var promises = [];
  promises.push(exclusive(scope, () => open(tempfile, 'w+').then(fd => {
    scope.fd = fd;
    return write(fd, buffer, 0, buffer.length, 0);
  }))
  .then(bytes => {
    t.equal(bytes, buffer.length);
  }));
  for(let i = 10; i-- > 0;) promises.push(shared(scope, () => {
    var buf = Buffer.allocUnsafe(buffer.length);
    return read(scope.fd, buf, 0, buf.length, 0)
    .then(bytes => {
      t.equal(bytes, buffer.length);
      t.same(buf, buffer);
    });
  }));
  promises.push(exclusive(scope, () => {
    return Promise.all([
      open(tempfile, 'w+').then(fd => {
        return write(fd, buffer2, 0, buffer2.length, 0).then(bytes => {
          scope.fd = fd;
          t.equal(bytes, buffer2.length);
        });
      }),
      close(scope.fd)
    ]);
  }));
  for(let i = 10; i-- > 0;) promises.push(shared(scope, () => {
    var buf = Buffer.allocUnsafe(buffer2.length);
    return read(scope.fd, buf, 0, buf.length, 0)
    .then(bytes => {
      t.equal(bytes, buffer2.length);
      t.same(buf, buffer2);
    });
  }));
  promises.push(exclusive(scope, () => {
    return close(scope.fd).then(() => {
      scope.fd = -1;
    })
  }));
  promises.push(exclusive(scope, () => {
    t.equal(scope.fd, -1);
    return unlink(tempfile);
  }));
  return Promise.all(promises)
  .then(() => t.ok(true))
  .catch(t.threw);
});


test('random', t => {
  t.plan(1+2*100+3);
  var status = {read: 0, unread: 0};
  var scope = {fd: undefined};
  var buffer = crypto.randomBytes(512*1024);
  var buffer2 = crypto.randomBytes(512*1024);
  var promises = [];
  for(let i = 100; i-- > 0;) promises.push(delay(Math.random()*100).then(() => {
    return shared(scope, () => {
      if (!scope.fd) {
        t.ok(true);
        t.ok(true);
        status.unread++;
      }
      else {
        var buf = Buffer.allocUnsafe(buffer.length);
        return read(scope.fd, buf, 0, buf.length, 0)
        .then(bytes => {
          t.equal(bytes, buffer.length);
          t.same(buf, buffer);
          status.read++;
        });
      }
    });
  }));

  promises.push(exclusive(scope, () => open(tempfile, 'w+').then(fd => {
    scope.fd = fd;
    return write(fd, buffer, 0, buffer.length, 0);
  }))
  .then(bytes => {
    t.equal(bytes, buffer.length);
  }));

  promises.push(delay(50).then(() => exclusive(scope, () => {
    return close(scope.fd).then(() => {
      return unlink(tempfile)
      .then(() => (scope.fd = null));
    })
  })));

  return Promise.all(promises)
  .then(() => {
    t.equal(scope.fd, null);
    t.ok(status.read > 30, "not ok, was: " + status.read);
    t.ok(status.unread > 30, "not ok, was: " + status.unread);
  })
  .catch(t.threw);
});
