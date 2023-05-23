/*
 *  Copyright (c) 2016-2023 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const path = require('path');
const fs = require('fs');
const raft = require('../..');
const { RaftPersistence } = raft.server;

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
    t.plan(34);
    var persistence = new RaftPersistence(path.join(workdir, 'one.persist'), []);
    t.type(persistence, RaftPersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.equal(persistence.currentTerm, 0);
      t.equal(persistence.votedFor, null);
      t.equal(persistence.peersUpdateRequest, null);
      t.equal(persistence.peersIndex, null);
      t.same(persistence.peers, []);
      t.equal(persistence[Symbol.for('byteSize')], 0);

      return persistence.update({currentTerm: 1});
    }).then(() => {
      t.equal(persistence.currentTerm, 1);
      t.equal(persistence.votedFor, null);
      t.equal(persistence.peersUpdateRequest, null);
      t.equal(persistence.peersIndex, null);
      t.same(persistence.peers, []);
      t.equal(persistence[Symbol.for('byteSize')], 14);

      return persistence.update({votedFor: 'me'});
    }).then(() => {
      t.equal(persistence.currentTerm, 1);
      t.equal(persistence.votedFor, 'me');
      t.equal(persistence.peersUpdateRequest, null);
      t.equal(persistence.peersIndex, null);
      t.same(persistence.peers, []);
      t.equal(persistence[Symbol.for('byteSize')], 27);

      return persistence.update({votedFor: null, currentTerm: 2});
    }).then(() => {
      t.equal(persistence.currentTerm, 2);
      t.equal(persistence.votedFor, null);
      t.equal(persistence.peersUpdateRequest, null);
      t.equal(persistence.peersIndex, null);
      t.same(persistence.peers, []);
      t.equal(persistence[Symbol.for('byteSize')], 51);

      return Promise.all([
        persistence.update({currentTerm: 3, votedFor: 'him'}),
        persistence.update({currentTerm: 4, peers: ['foo','bar','baz'], peersUpdateRequest: 'AAAAAAAAAAAAAAAA', peersIndex: 77})
      ]);
    }).then(() => {
      t.equal(persistence.currentTerm, 4);
      t.equal(persistence.votedFor, 'him');
      t.equal(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.equal(persistence.peersIndex, 77);
      t.same(persistence.peers, ['foo','bar','baz']);
      t.equal(persistence[Symbol.for('byteSize')], 159);

      return persistence.close();
    }).then(() => {
      t.equal(fs.statSync(persistence.filename).size, 159);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.test('test existing persistence', t => {
    t.plan(16);
    var persistence = new RaftPersistence(path.join(workdir, 'one.persist'), []);
    t.type(persistence, RaftPersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.equal(persistence.currentTerm, 4);
      t.equal(persistence.votedFor, 'him');
      t.equal(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.equal(persistence.peersIndex, 77);
      t.same(persistence.peers, ['foo','bar','baz']);
      t.equal(persistence[Symbol.for('byteSize')], 159);

      return persistence.update({votedFor: 'foo', currentTerm: 5});
    }).then(() => {
      t.equal(persistence.currentTerm, 5);
      t.equal(persistence.votedFor, 'foo');
      t.equal(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.equal(persistence.peersIndex, 77);
      t.same(persistence.peers, ['foo','bar','baz']);
      t.equal(persistence[Symbol.for('byteSize')], 186);

      return persistence.close();
    }).then(() => {
      t.equal(fs.statSync(persistence.filename).size, 186);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.test('test rotate', t => {
    t.plan(16);
    var persistence = new RaftPersistence(path.join(workdir, 'one.persist'), []);
    t.type(persistence, RaftPersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.equal(persistence.currentTerm, 5);
      t.equal(persistence.votedFor, 'foo');
      t.equal(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.equal(persistence.peersIndex, 77);
      t.same(persistence.peers, ['foo','bar','baz']);
      t.equal(persistence[Symbol.for('byteSize')], 186);

      return persistence.rotate({currentTerm: 6});
    }).then(() => {
      t.equal(persistence.currentTerm, 6);
      t.equal(persistence.votedFor, 'foo');
      t.equal(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.equal(persistence.peersIndex, 77);
      t.same(persistence.peers, ['foo','bar','baz']);
      t.equal(persistence[Symbol.for('byteSize')], 94);

      return persistence.close();
    }).then(() => {
      t.equal(fs.statSync(persistence.filename).size, 94);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.test('test auto rotate', t => {
    t.plan(8 + 6 + 2);
    var bigString = Buffer.allocUnsafe(64*1024).toString('hex');
    var persistence = new RaftPersistence(path.join(workdir, 'one.persist'), []);
    t.type(persistence, RaftPersistence);
    t.type(persistence.ready, 'function');
    return persistence.ready().then(() => {
      t.equal(persistence.currentTerm, 6);
      t.equal(persistence.votedFor, 'foo');
      t.equal(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
      t.equal(persistence.peersIndex, 77);
      t.same(persistence.peers, ['foo','bar','baz']);
      t.equal(persistence[Symbol.for('byteSize')], 94);

      var bytesize = 0;
      var expectedTerm = 11;
      var next = () => {
        if (persistence[Symbol.for('byteSize')] > bytesize) {
          bytesize = persistence[Symbol.for('byteSize')];
          return persistence.update({currentTerm: persistence.currentTerm + 1, votedFor: bigString}).then(next);
        } else {
          t.equal(persistence.currentTerm, expectedTerm);
          t.equal(persistence[Symbol.for('byteSize')], 131167);
          t.equal(persistence.votedFor, bigString);
          t.equal(persistence.peersUpdateRequest, 'AAAAAAAAAAAAAAAA');
          t.equal(persistence.peersIndex, 77);
          t.same(persistence.peers, ['foo','bar','baz']);
          return persistence.close();
        }
      };
      return next();
    }).then(() => {
      t.equal(fs.statSync(persistence.filename).size, 131167);
    })
    .then(() => t.ok(true)).catch(t.threw);
  });

  suite.end();
});
