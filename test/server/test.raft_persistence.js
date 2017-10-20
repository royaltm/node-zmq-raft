/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const path = require('path');
const fs = require('fs');
const raft = require('../..');
const { RaftPersistence } = raft.server;
const { unlink } = raft.utils.fsutil;

var workdir = fs.mkdtempSync(path.resolve(__dirname, '..', '..', 'tmp') + path.sep);

process.on('exit', () => {
  fs.readdirSync(workdir).forEach(file => fs.unlinkSync(path.join(workdir, file)));
  fs.rmdirSync(workdir);
});

test('should be a function', t => {
  t.type(RaftPersistence, 'function');
  t.end();
});

test('RaftPersistence', suite => {

  suite.test('test new persistence', t => {
    t.plan(29);
    var persistence = new RaftPersistence(path.join(workdir, 'one.persist'), []);
    t.type(persistence, RaftPersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.strictEquals(persistence.currentTerm, 0);
      t.strictEquals(persistence.votedFor, null);
      t.strictEquals(persistence.peersUpdateRequest, null);
      t.deepEquals(persistence.peers, []);
      t.strictEquals(persistence[Symbol.for('byteSize')], 0);

      return persistence.update({currentTerm: 1});
    }).then(() => {
      t.strictEquals(persistence.currentTerm, 1);
      t.strictEquals(persistence.votedFor, null);
      t.strictEquals(persistence.peersUpdateRequest, null);
      t.deepEquals(persistence.peers, []);
      t.strictEquals(persistence[Symbol.for('byteSize')], 14);

      return persistence.update({votedFor: 'me'});
    }).then(() => {
      t.strictEquals(persistence.currentTerm, 1);
      t.strictEquals(persistence.votedFor, 'me');
      t.strictEquals(persistence.peersUpdateRequest, null);
      t.deepEquals(persistence.peers, []);
      t.strictEquals(persistence[Symbol.for('byteSize')], 27);

      return persistence.update({votedFor: null, currentTerm: 2});
    }).then(() => {
      t.strictEquals(persistence.currentTerm, 2);
      t.strictEquals(persistence.votedFor, null);
      t.strictEquals(persistence.peersUpdateRequest, null);
      t.deepEquals(persistence.peers, []);
      t.strictEquals(persistence[Symbol.for('byteSize')], 51);

      return Promise.all([
        persistence.update({currentTerm: 3, votedFor: 'him'}),
        persistence.update({currentTerm: 4, peers: ['foo','bar','baz'], peersUpdateRequest: 'AAAAAAAAAAAAAAAA'})
      ]);
    }).then(() => {
      t.strictEquals(persistence.currentTerm, 4);
      t.strictEquals(persistence.votedFor, 'him');
      t.strictEquals(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.deepEquals(persistence.peers, ['foo','bar','baz']);
      t.strictEquals(persistence[Symbol.for('byteSize')], 147);

      return persistence.close();
    }).then(() => {
      t.strictEquals(fs.statSync(persistence.filename).size, 147);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.test('test existing persistence', t => {
    t.plan(14);
    var persistence = new RaftPersistence(path.join(workdir, 'one.persist'), []);
    t.type(persistence, RaftPersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.strictEquals(persistence.currentTerm, 4);
      t.strictEquals(persistence.votedFor, 'him');
      t.strictEquals(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.deepEquals(persistence.peers, ['foo','bar','baz']);
      t.strictEquals(persistence[Symbol.for('byteSize')], 147);

      return persistence.update({votedFor: 'foo', currentTerm: 5});
    }).then(() => {
      t.strictEquals(persistence.currentTerm, 5);
      t.strictEquals(persistence.votedFor, 'foo');
      t.strictEquals(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.deepEquals(persistence.peers, ['foo','bar','baz']);
      t.strictEquals(persistence[Symbol.for('byteSize')], 174);

      return persistence.close();
    }).then(() => {
      t.strictEquals(fs.statSync(persistence.filename).size, 174);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.test('test rotate', t => {
    t.plan(14);
    var persistence = new RaftPersistence(path.join(workdir, 'one.persist'), []);
    t.type(persistence, RaftPersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.strictEquals(persistence.currentTerm, 5);
      t.strictEquals(persistence.votedFor, 'foo');
      t.strictEquals(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.deepEquals(persistence.peers, ['foo','bar','baz']);
      t.strictEquals(persistence[Symbol.for('byteSize')], 174);

      return persistence.rotate({currentTerm: 6});
    }).then(() => {
      t.strictEquals(persistence.currentTerm, 6);
      t.strictEquals(persistence.votedFor, 'foo');
      t.strictEquals(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.deepEquals(persistence.peers, ['foo','bar','baz']);
      t.strictEquals(persistence[Symbol.for('byteSize')], 82);

      return persistence.close();
    }).then(() => {
      t.strictEquals(fs.statSync(persistence.filename).size, 82);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.test('test auto rotate', t => {
    t.plan(7 + 5 + 2);
    var bigString = Buffer.allocUnsafe(64*1024).toString('hex');
    var persistence = new RaftPersistence(path.join(workdir, 'one.persist'), []);
    t.type(persistence, RaftPersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.strictEquals(persistence.currentTerm, 6);
      t.strictEquals(persistence.votedFor, 'foo');
      t.strictEquals(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.deepEquals(persistence.peers, ['foo','bar','baz']);
      t.strictEquals(persistence[Symbol.for('byteSize')], 82);

      var bytesize = 0;
      var expectedTerm = 11;
      var next = () => {
        if (persistence[Symbol.for('byteSize')] > bytesize) {
          bytesize = persistence[Symbol.for('byteSize')];
          return persistence.update({currentTerm: persistence.currentTerm + 1, votedFor: bigString}).then(next);
        } else {
          t.strictEquals(persistence.currentTerm, expectedTerm);
          t.strictEquals(persistence[Symbol.for('byteSize')], 131155);
          t.strictEquals(persistence.votedFor, bigString);
          t.strictEquals(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
          t.deepEquals(persistence.peers, ['foo','bar','baz']);
          return persistence.close();
        }
      };
      return next();
    }).then(() => {
      t.strictEquals(fs.statSync(persistence.filename).size, 131155);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.end();
});
