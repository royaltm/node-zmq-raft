/* 
 *  Copyright (c) 2017 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const fs = require('fs')
    , path = require('path')
    , crypto = require('crypto')
    , { Readable } = require('stream')
    , mp = require('msgpack-lite')
const test = require('tap').test;

const raft = require('../..');
const { SnapshotFile } = raft.common;
const { tokenfile: { TokenFile } } = raft.utils;

// const tempDir = fs.mkdtempSync(path.join(__dirname, '..', '..', 'tmp', 'idxfile.'));
var tempDir = fs.mkdtempSync(path.resolve(__dirname, '..', '..', 'tmp', 'snap.'));

process.on('exit', () => {
  fs.readdirSync(tempDir).forEach(file => fs.unlinkSync(path.join(tempDir, file)));
  fs.rmdirSync(tempDir);
});

test('SnapshotFile', suite => {

  var data = crypto.randomBytes(199);

  test('should create snapshot file', t => {
    t.plan(13);
    var snapshot = new SnapshotFile(path.join(tempDir, 'snap'), 42, 100, data.length);
    t.type(snapshot, SnapshotFile);
    return snapshot.ready()
    .then(snapshot => {
      t.type(snapshot, SnapshotFile);
      t.strictEquals(snapshot.filename, path.join(tempDir, 'snap'));
      t.type(snapshot.dataOffset, 'number');
      t.ok(snapshot.dataOffset > 44);
      t.strictEquals(snapshot.logIndex, 42);
      t.strictEquals(snapshot.logTerm, 100);
      t.strictEquals(snapshot.dataSize, data.length);
      t.strictEquals(snapshot.isClosed, false);
      return snapshot.write(data, 0, data.length);
    })
    .then(written => {
      t.strictEquals(written, data.length);
      return snapshot.sync();
    })
    .then(() => snapshot.read(0, data.length))
    .then(buffer => {
      t.type(buffer, Buffer);
      t.ok(buffer.equals(data));
      return snapshot.close();
    })
    .then(() => {
      t.strictEquals(snapshot.isClosed, true);
    }).catch(t.threw);
  });

  test('should open snapshot file', t => {
    t.plan(25);
    var snapshot = new SnapshotFile(path.join(tempDir, 'snap'));
    t.type(snapshot, SnapshotFile);
    return snapshot.ready()
    .then(snapshot => {
      t.type(snapshot, SnapshotFile);
      t.strictEquals(snapshot.filename, path.join(tempDir, 'snap'));
      t.type(snapshot.dataOffset, 'number');
      t.ok(snapshot.dataOffset > 44);
      t.strictEquals(snapshot.logIndex, 42);
      t.strictEquals(snapshot.logTerm, 100);
      t.strictEquals(snapshot.dataSize, data.length);
      t.strictEquals(snapshot.isClosed, false);
      var buffer = Buffer.allocUnsafeSlow(data.length);
      return snapshot.read(0, data.length, buffer)
      .then(buf => {
        t.type(buf, Buffer);
        t.strictEquals(buf, buffer);
        t.strictEquals(buffer.equals(data), true);
        return snapshot.read(0, 25, buffer, 100)
      })
      .then(buf => {
        t.type(buf, Buffer);
        t.notStrictEquals(buf, buffer);
        t.strictEquals(buf.buffer, buffer.buffer);
        t.strictEquals(buf.offset, 100);
        t.strictEquals(buf.length, 25);
        t.strictEquals(buf.compare(data, 0, 25), 0);
      });
    })
    .then(() => new Promise((resolve, reject) => {
      snapshot.createDataReadStream(100).on('error', reject).on('end', resolve)
      .on('data', chunk => {
        try {
          t.type(chunk, Buffer);
          t.strictEquals(chunk.compare(data, 100), 0);
        } catch(err) { reject(err) }
      });
    }))
    .then(() => {
      var tokenfile = snapshot.makeTokenFile();
      t.type(tokenfile, TokenFile);
      return tokenfile.readTokenData('META', 0, snapshot.dataOffset);
    })
    .then(data => {
      t.type(data, Buffer);
      var meta = mp.decode(data);
      t.type(meta, Object);
      t.deepEquals(Object.keys(meta), ['created', 'hostname']);
      return snapshot.close();
    })
    .then(() => {
      t.strictEquals(snapshot.isClosed, true);
    }).catch(t.threw);
  });

  test('should create snapshot file from stream', t => {
    t.plan(21);
    var snapshot = new SnapshotFile(path.join(tempDir, 'snap2'), 77, 1, createDataStream(data, 100));
    t.type(snapshot, SnapshotFile);
    return snapshot.ready()
    .then(snapshot => {
      t.type(snapshot, SnapshotFile);
      t.strictEquals(snapshot.filename, path.join(tempDir, 'snap2'));
      t.type(snapshot.dataOffset, 'number');
      t.ok(snapshot.dataOffset > 44);
      t.strictEquals(snapshot.logIndex, 77);
      t.strictEquals(snapshot.logTerm, 1);
      t.strictEquals(snapshot.dataSize, 100*data.length);
      t.strictEquals(snapshot.isClosed, false);
      t.deepEquals(fs.readdirSync(tempDir), ['snap', 'snap2']);
      return snapshot.replace(path.join(tempDir, 'snap'));
    })
    .then(name => {
      t.strictEquals(name, path.join(tempDir, 'snap'))
      t.strictEquals(snapshot.logIndex, 77);
      t.strictEquals(snapshot.logTerm, 1);
      t.strictEquals(snapshot.dataSize, 100*data.length);
      t.strictEquals(snapshot.filename, path.join(tempDir, 'snap'));
      var files = fs.readdirSync(tempDir);
      t.strictEquals(files.length, 2);
      t.strictEquals(files[0], 'snap');
      t.matches(files[1], /^snap-\d{4}-\d\d-\d\d-\d{6}-\d{3}/);
      return snapshot.read(data.length, data.length);
    })
    .then(buffer => {
      t.type(buffer, Buffer);
      t.ok(buffer.equals(data));
       return snapshot.close();
    })
    .then(() => {
      t.strictEquals(snapshot.isClosed, true);
    }).catch(t.threw);
  });

  suite.end();
});

function createDataStream(data, count) {
  return new Readable({
    read() {
      if (count--) {
        setImmediate(() => {
          this.push(data);
        });
      }
      else this.push(null);
    }
  });
}