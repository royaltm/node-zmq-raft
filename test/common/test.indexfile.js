/*
 *  Copyright (c) 2017-2023 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const fs = require('fs')
    , path = require('path')
    , crypto = require('crypto')
const test = require('tap').test;

const raft = require('../..');
const { IndexFile } = raft.common;

const tempDir = fs.mkdtempSync(path.join(__dirname, '..', '..', 'tmp', 'idxfile.'));

process.on('exit', () => {
  (function rmdeep(dir) {
    fs.readdirSync(dir).forEach(item => rmdeep(path.join(dir, item)));
    fs.rmdirSync(dir);
  })(tempDir);
});


test('IndexFile', suite => {

  suite.test('should create index file', t => {
    t.plan(10);
    return new IndexFile(tempDir, 1).ready()
    .then(indexFile => {
      var filename = path.join(tempDir, '00000', '00', '00', '00000000000001.rlog');
      t.type(indexFile, IndexFile);
      t.equal(indexFile.capacity, IndexFile.DEFAULT_CAPACITY - 1);
      t.equal(indexFile.index, 1);
      t.equal(indexFile.nextIndex, 1);
      t.equal(indexFile.free, indexFile.capacity);
      t.equal(indexFile.firstAllowedIndex, 1);
      t.equal(indexFile.lastAllowedIndex, indexFile.capacity);
      t.equal(indexFile.basename, '00000000000001');
      t.equal(indexFile.filename, filename);
      t.ok(fs.existsSync(filename));
      return indexFile.close()
    })
    .catch(t.threw);
  });

  suite.test('should create another index file', t => {
    t.plan(21);
    return new IndexFile(tempDir, 0x123456).ready()
    .then(indexFile => {
      var filename = path.join(tempDir, '00000', '00', '01', '00000000123456.rlog');
      t.type(indexFile, IndexFile);
      t.equal(indexFile.capacity, IndexFile.DEFAULT_CAPACITY - 0x123456 % IndexFile.DEFAULT_CAPACITY);
      t.equal(indexFile.index, 0x123456);
      t.equal(indexFile.nextIndex, 0x123456);
      t.equal(indexFile.free, indexFile.capacity);
      t.equal(indexFile.firstAllowedIndex, 0x123456);
      t.equal(indexFile.lastAllowedIndex, 0x123456 + indexFile.capacity - 1);
      t.equal(indexFile.basename, '00000000123456');
      t.equal(indexFile.filename, filename);
      t.ok(fs.existsSync(filename));

      return indexFile.append(Buffer.from([42]))
      .then(([numUnwrittenEntries, indexOfNextEntry]) => {
        t.equal(numUnwrittenEntries, 0);
        t.equal(indexOfNextEntry, 0x123457);
        t.equal(indexFile.capacity, IndexFile.DEFAULT_CAPACITY - 0x123456 % IndexFile.DEFAULT_CAPACITY);
        t.equal(indexFile.index, 0x123456);
        t.equal(indexFile.nextIndex, 0x123457);
        t.equal(indexFile.free, indexFile.capacity - 1);
        t.equal(indexFile.firstAllowedIndex, 0x123456);
        t.equal(indexFile.lastAllowedIndex, 0x123456 + indexFile.capacity - 1);
        return indexFile.read(0x123456, 1);
      })
      .then(entry => {
        t.type(entry, Buffer);
        t.equal(entry.length, 1);
        t.equal(entry[0], 42);
        return indexFile.close();
      });
    })
    .catch(t.threw);
  });

  suite.test('should open existing index file', t => {
    t.plan(14);
    var filename = path.join(tempDir, '00000', '00', '01', '00000000123456.rlog');
    return new IndexFile(filename).ready()
    .then(indexFile => {
      t.type(indexFile, IndexFile);
      t.equal(indexFile.capacity, IndexFile.DEFAULT_CAPACITY - 0x123456 % IndexFile.DEFAULT_CAPACITY);
      t.equal(indexFile.index, 0x123456);
      t.equal(indexFile.nextIndex, 0x123457);
      t.equal(indexFile.free, indexFile.capacity - 1);
      t.equal(indexFile.firstAllowedIndex, 0x123456);
      t.equal(indexFile.lastAllowedIndex, 0x123456 + indexFile.capacity - 1);
      t.equal(indexFile.basename, '00000000123456');
      t.equal(indexFile.filename, filename);
      t.ok(fs.existsSync(filename));

      return indexFile.read(0x123456, 1)
      .then(entry => {
        t.type(entry, Buffer);
        t.equal(entry.length, 1);
        t.equal(entry[0], 42);
        return indexFile.destroy();
      })
      .then(() => {
        t.ok(!fs.existsSync(filename));
      });
    })
    .catch(t.threw);
  });

  suite.test('should open existing index file and flood it', t => {
    t.plan(9 + 6 + 100 * 6 + 3 + 2 + (IndexFile.DEFAULT_CAPACITY - 1) * 4
          + (IndexFile.DEFAULT_CAPACITY - 1) * 2 + 6 + 7);
    var filename = path.join(tempDir, '00000', '00', '00', '00000000000001.rlog');
    var entries = [];
    return new IndexFile(filename).ready()
    .then(indexFile => {
      t.type(indexFile, IndexFile);
      t.equal(indexFile.capacity, IndexFile.DEFAULT_CAPACITY - 1);
      t.equal(indexFile.index, 1);
      t.equal(indexFile.nextIndex, 1);
      t.equal(indexFile.free, indexFile.capacity);
      t.equal(indexFile.firstAllowedIndex, 1);
      t.equal(indexFile.lastAllowedIndex, indexFile.capacity);
      t.equal(indexFile.basename, '00000000000001');
      t.equal(indexFile.filename, filename);

      for(let i = indexFile.capacity - 100; i-- > 0;) {
        entries.push(crypto.randomBytes((Math.random()*100 + 1)>>>0));
      }

      const append = () => {
        var entry = crypto.randomBytes((Math.random()*100 + 1)>>>0);
        return indexFile.append(entry)
        .then(([numUnwrittenEntries, indexOfNextEntry]) => {
          if (numUnwrittenEntries === 1) {
            t.equal(indexOfNextEntry, indexFile.lastAllowedIndex + 1);
            t.equal(indexFile.nextIndex, indexOfNextEntry);
            t.equal(indexFile.free, 0);
          }
          else {
            entries.push(entry);
            t.equal(numUnwrittenEntries, 0);
            t.equal(indexOfNextEntry, indexFile.nextIndex);
            t.equal(indexFile.capacity, IndexFile.DEFAULT_CAPACITY - 1);
            t.equal(indexFile.index, 1);
            t.equal(indexFile.nextIndex, entries.length + 1);
            t.equal(indexFile.free, indexFile.capacity - entries.length);
            return append();
          }
        });
      };

      return indexFile.append(entries)
      .then(([numUnwrittenEntries, indexOfNextEntry]) => {
        t.equal(numUnwrittenEntries, 0);
        t.equal(indexOfNextEntry, indexFile.nextIndex);
        t.equal(indexFile.capacity, IndexFile.DEFAULT_CAPACITY - 1);
        t.equal(indexFile.index, 1);
        t.equal(indexFile.nextIndex, entries.length + 1);
        t.equal(indexFile.free, indexFile.capacity - entries.length);
        return append();
      })
      .then(() => indexFile.readv(1, indexFile.capacity))
      .then(readv => {
        t.type(readv, Array);
        t.equal(readv.length, entries.length);
        entries.forEach((entry, index) => {
          t.type(readv[0], Buffer);
          t.ok(readv[index].equals(entry));
        });

        return Promise.all([
          new Promise((resolve, reject) => {
            var input = [], index = 0;
            var extractor = indexFile.createEntryExtractor(input, indexFile.firstAllowedIndex, indexFile.lastAllowedIndex)
            indexFile.createLogReadStream(indexFile.firstAllowedIndex, indexFile.lastAllowedIndex, 65536)
            .on('error', reject)
            .on('end', () => {
              try {
                t.ok(extractor.next().done)
                t.equal(input.length, 0);
                t.equal(entries.length, index);
                resolve();
              } catch(err) { reject(err); }
            })
            .on('data', data => {
              try {
                input.push(data);
                for(;;) {
                  let {value, done} = extractor.next();
                  if (done) {
                    t.equal(input.length, 0);
                    t.equal(entries.length, index);
                  }
                  if (value === undefined) break;
                  t.type(value, Buffer);
                  t.ok(value.equals(entries[index++]));
                }
              } catch(err) { reject(err); }
            });
          }),
          new Promise((resolve, reject) => {
            var index = 0;
            indexFile.createLogEntryReadStream(indexFile.firstAllowedIndex, indexFile.lastAllowedIndex, 32768)
            .on('error', reject)
            .on('end', () => {
              try {
                t.equal(entries.length, index);
                resolve();
              } catch(err) { reject(err); }
            })
            .on('data', data => {
              try {
                t.type(data, Buffer);
                t.ok(data.equals(entries[index++]));
              } catch(err) { reject(err); }
            });
          })
        ]);
      })
      .then(() => indexFile.truncate(1))
      .then(() => {
        t.type(indexFile, IndexFile);
        t.equal(indexFile.capacity, IndexFile.DEFAULT_CAPACITY - 1);
        t.equal(indexFile.index, 1);
        t.equal(indexFile.nextIndex, 1);
        t.equal(indexFile.free, indexFile.capacity);
        t.ok(fs.existsSync(filename));
        return indexFile.destroy()
      })
      .then(() => {
        t.ok(!fs.existsSync(filename));
      });
    })
    .catch(t.threw);
  });

  suite.end();
});
