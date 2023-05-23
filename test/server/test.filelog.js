/*
 *  Copyright (c) 2017-2023 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const fs = require('fs')
    , path = require('path')
    , { Readable } = require('stream')
    , crypto = require('crypto')
const test = require('tap').test;

const raft = require('../..');
const { LogEntry, SnapshotChunk, IndexFile } = raft.common;
const { DEFAULT_CAPACITY } = IndexFile;
const { FileLog } = raft.server;
const { id: { genIdent }
      , fsutil: { readdir } } = raft.utils;

const { readers, LOG_ENTRY_HEADER_SIZE
      , LOG_ENTRY_TYPE_STATE
      , LOG_ENTRY_TYPE_CONFIG
      , LOG_ENTRY_TYPE_CHECKPOINT } = LogEntry;

const tempDir = fs.mkdtempSync(path.join(__dirname, '..', '..', 'tmp', 'log.'));
const tempFiles = [];

process.on('exit', () => {
  tempFiles.forEach(file => fs.unlinkSync(path.join(tempDir, file)));
  (function rmdeep(dir) {
    fs.readdirSync(dir).forEach(item => rmdeep(path.join(dir, item)));
    fs.rmdirSync(dir);
  })(tempDir);
});


test('FileLog', suite => {

  const TOTAL_ENTRIES = 1000;

  var log, reqestKey0, reqestKey1, logentries, digest;

  suite.test('should not create a FileLog', t => {
    t.plan(1);
    t.throws(() =>
      new FileLog(path.join(tempDir, 'log'), path.join(tempDir, 'snap'), {
                        requestIdTtl: null, requestIdCacheMax: null}),
      new Error("FileLog: options requestIdTtl and requestIdCacheMax must not be null at the same time"));
  });

  suite.test('should create FileLog', t => {
    t.plan(87);
    log = new FileLog(path.join(tempDir, 'log'), path.join(tempDir, 'snap'));
    t.type(log, FileLog);
    t.equal(log.requestIdTtl, raft.common.constants.DEFAULT_REQUEST_ID_TTL);
    return log.ready()
    .then(filelog => {
      t.equal(filelog, log);
      t.equal(log.logdir, path.join(tempDir, 'log'));
      t.equal(log.firstIndex, 1);
      t.equal(log.lastIndex, 0);
      t.equal(log.lastTerm, 0);
      t.equal(log.indexFileCapacity, DEFAULT_CAPACITY);
      t.equal(log.snapshot.logIndex, 0);
      t.equal(log.snapshot.logTerm, 0);
      t.equal(log.getFirstFreshIndex(), undefined);
      return FileLog.readIndexFileNames(log.logdir);
    })
    .then(files => {
      t.same(files, [path.join(tempDir, 'log', '00000', '00', '00', '00000000000001.rlog')]);
      return log.findIndexFilePathOf(0);
    })
    .then(filepath => {
      t.equal(filepath, undefined);
      return log.findIndexFilePathOf(1);
    })
    .then(filepath => {
      t.equal(filepath, undefined);

      return log.appendCheckpoint(42);
    })
    .then(index => {
      t.equal(index, 1);
      t.equal(log.firstIndex, 1);
      t.equal(log.lastIndex, 1);
      t.equal(log.lastTerm, 42);
      t.equal(log.getFirstFreshIndex(), undefined);

      return log.findIndexFilePathOf(1);
    })
    .then(filepath => {
      t.equal(filepath, path.join(tempDir, 'log', '00000', '00', '00', '00000000000001.rlog'));
      return log.findIndexFilePathOf(2);
    })
    .then(filepath => {
      t.equal(filepath, undefined);

      reqestKey0 = genIdent('base64');
      return log.appendState(reqestKey0, 43, Buffer.from('foo'));
    })
    .then(index => {
      t.equal(index, 2);
      t.equal(log.firstIndex, 1);
      t.equal(log.lastIndex, 2);
      t.equal(log.lastTerm, 43);
      t.equal(log.getFirstFreshIndex(), 2);
      t.equal(log.getRid(reqestKey0), 2);

      return log.findIndexFilePathOf(2);
    })
    .then(filepath => {
      t.equal(filepath, path.join(tempDir, 'log', '00000', '00', '00', '00000000000001.rlog'));

      reqestKey1 = genIdent('base64');
      return log.appendConfig(reqestKey1, 43, Buffer.from('bar'));
    })
    .then(index => {
      t.equal(index, 3);
      t.equal(log.firstIndex, 1);
      t.equal(log.lastIndex, 3);
      t.equal(log.lastTerm, 43);
      t.equal(log.getFirstFreshIndex(), 2);
      t.equal(log.getRid(reqestKey0), 2);
      t.equal(log.getRid(reqestKey1), 3);
      return Promise.all([log.termAt(4), log.termAt(3), log.termAt(2), log.termAt(1)]);
    })
    .then(terms => {
      t.same(terms, [undefined, 43, 43, 42]);
      var buffer = Buffer.allocUnsafeSlow(LOG_ENTRY_HEADER_SIZE + 3);
      return log.getEntry(3, buffer)
      .then(buf => {
        t.type(buf, Buffer);
        t.not(buf, buffer);
        t.equal(buf.offset, 0);
        t.equal(buf.length, buffer.length);
        t.equal(buf.buffer, buffer.buffer);
        t.equal(readers.readRequestIdOf(buf, 'base64'), reqestKey1);
        t.equal(readers.readTypeOf(buf), LOG_ENTRY_TYPE_CONFIG);
        t.equal(readers.readTermOf(buf), 43);
        t.equal(readers.readDataOf(buf).toString(), 'bar');

        return Promise.all([log.getEntry(2), log.getEntry(1), log.getEntries(1, 2)]);
      })
    })
    .then(entries => {
      t.type(entries, Array);
      t.equal(entries.length, 3);
      t.type(entries[0], Buffer);
      t.type(entries[1], Buffer);
      t.type(entries[2], Array);
      t.equal(entries[2].length, 2);
      t.equal(entries[2][0].equals(entries[1]), true);
      t.equal(entries[2][1].equals(entries[0]), true);
      t.equal(readers.readRequestIdOf(entries[0], 'base64'), reqestKey0);
      t.equal(readers.readTypeOf(entries[0]), LOG_ENTRY_TYPE_STATE);
      t.equal(readers.readTermOf(entries[0]), 43);
      t.equal(readers.readDataOf(entries[0]).toString(), 'foo');
      t.equal(readers.readRequestIdOf(entries[1], 'base64'), 'AAAAAAAAAAAAAAAA');
      t.equal(readers.readTypeOf(entries[1]), LOG_ENTRY_TYPE_CHECKPOINT);
      t.equal(readers.readTermOf(entries[1]), 42);
      t.equal(readers.readDataOf(entries[1]).length, 1);
      t.equal(readers.readDataOf(entries[1])[0], 0xc0);
      var buffer = Buffer.allocUnsafeSlow(LOG_ENTRY_HEADER_SIZE*2 + 6);
      return log.readEntries(1, 3, buffer)
      .then(entries => {
        t.type(entries, Array);
        t.equal(entries.length, 2);
        t.type(entries[0], Buffer);
        t.not(entries[0], buffer);
        t.equal(entries[0].buffer, buffer.buffer);
        t.equal(entries[0].offset, 0);
        t.equal(entries[0].length, LOG_ENTRY_HEADER_SIZE + 1);
        t.equal(readers.readDataOf(entries[0])[0], 0xc0);
        t.type(entries[1], Buffer);
        t.not(entries[1], buffer);
        t.equal(entries[1].buffer, buffer.buffer);
        t.equal(entries[1].offset, LOG_ENTRY_HEADER_SIZE + 1);
        t.equal(entries[1].length, LOG_ENTRY_HEADER_SIZE + 3);
        t.equal(readers.readDataOf(entries[1]).toString(), 'foo');
        buffer = Buffer.allocUnsafeSlow(3).fill('bzz');
        return log.readEntries(1, 3, buffer)
      })
      .then(entries => {
        t.type(entries, Array);
        t.equal(entries.length, 1);
        t.type(entries[0], Buffer);
        t.not(entries[0], buffer);
        t.not(entries[0].buffer, buffer.buffer);
        t.equal(entries[0].length, LOG_ENTRY_HEADER_SIZE + 1);
        t.equal(readers.readDataOf(entries[0])[0], 0xc0);
        t.equal(buffer.toString(), 'bzz');
      });
    })
    .then(() => {
      var start = Date.now()
      return Promise.all([
        start,
        log.appendState(genIdent("buffer"), 44, crypto.randomBytes(1)).then(logIndex => [logIndex, Date.now()]),
        log.appendState(genIdent("buffer"), 44, crypto.randomBytes(100000)).then(logIndex => [logIndex, Date.now()]),
        log.appendState(genIdent("buffer"), 44, crypto.randomBytes(100000)).then(logIndex => [logIndex, Date.now()]),
        log.appendState(genIdent("buffer"), 44, crypto.randomBytes(100000)).then(logIndex => [logIndex, Date.now()]),
        log.appendState(genIdent("buffer"), 44, crypto.randomBytes(100000)).then(logIndex => [logIndex, Date.now()])
      ]);
    })
    .then(result => {
      var indexes = result.slice(1);
      t.same(indexes.map(([logIndex]) => logIndex), [4,5,6,7,8]);
      var start = result[0];
      var deltas = indexes.map(([_,ts]) => { var delta = ts - start; start = ts; return delta; });
      var sum = deltas.slice(1).reduce((sum, delta) => sum+delta);
      t.ok(deltas[0] > 0, 'first append should be few ms later');
      t.ok(sum < deltas[0], 'the rest should be relatively small comparing to first append');

      return log.close();
    }).catch(err => {
      log.close();
      t.threw(err);
    });
  });

  suite.test('should open FileLog', t => {
    t.plan(20 + TOTAL_ENTRIES*4 + 11);
    log = new FileLog(path.join(tempDir, 'log'), path.join(tempDir, 'snap'));
    t.type(log, FileLog);
    t.equal(log.requestIdTtl, raft.common.constants.DEFAULT_REQUEST_ID_TTL);
    return log.ready()
    .then(filelog => {
      t.equal(filelog, log);
      t.equal(log.logdir, path.join(tempDir, 'log'));
      t.equal(log.firstIndex, 1);
      t.equal(log.lastIndex, 8);
      t.equal(log.lastTerm, 44);
      t.equal(log.indexFileCapacity, DEFAULT_CAPACITY);
      t.equal(log.snapshot.logIndex, 0);
      t.equal(log.snapshot.logTerm, 0);
      t.equal(log.snapshot.dataSize, 0);
      t.equal(log.getFirstFreshIndex(), 2);
      t.equal(log.getRid(reqestKey0), 2);
      t.equal(log.getRid(reqestKey1), 3);

      var entries = Array.from(new Array(TOTAL_ENTRIES)).map((_, i) => randomEntry(10000, i + 112));
      var size = entries.reduce((size, buf) => size + buf.length, 0);
      var buffer = Buffer.allocUnsafeSlow(size);
      return log.appendEntries(entries, 1)
      .then(() => log.readEntries(1, TOTAL_ENTRIES, buffer))
      .then(result => {
        t.equal(log.firstIndex, 1);
        t.equal(log.lastIndex, TOTAL_ENTRIES);
        t.equal(log.lastTerm, 1111);
        t.equal(log.getFirstFreshIndex(), 1);
        t.type(result, Array);
        t.equal(result.length, TOTAL_ENTRIES);
        result.forEach((entry, i) => {
          t.equal(entry.buffer, buffer.buffer);
          t.type(entry, Buffer);
          t.equal(entry.equals(entries[i]), true);
          t.equal(log.getRid(readers.readRequestIdOf(entry, 'base64')), i + 1);
        });

        var snapnew = path.join(tempDir, 'compact', 'snap.new');

        return Promise.all([
          log.watchInstallSnapshot(snapnew)
             .then(() => new Promise((resolve, reject) => {
                log.on('error', reject).once('snapshot', resolve);
             })),
          log.createTmpSnapshot(TOTAL_ENTRIES, 1111, 0).ready()
             .then(snapshot => snapshot.replace(snapnew).then(() => snapshot.close()))
        ]);
      });
    })
    .then(() => {
      t.equal(log.firstIndex, TOTAL_ENTRIES + 1);
      t.equal(log.lastIndex, TOTAL_ENTRIES);
      t.equal(log.lastTerm, 1111);
      t.equal(log.snapshot.filename, path.join(tempDir, 'snap'));
      t.equal(log.snapshot.logIndex, TOTAL_ENTRIES);
      t.equal(log.snapshot.logTerm, 1111);
      t.equal(log.snapshot.dataSize, 0);
      t.equal(log.getFirstFreshIndex(), 1);
      return FileLog.readIndexFileNames(log.logdir);
    })
    .then(files => {
      t.equal(files.length, 1);
      t.equal(files[0], path.join(tempDir, 'log', '00000', '00', '00', '00000000000001.rlog'));
      return log.findIndexFilePathOf(TOTAL_ENTRIES);
    })
    .then(filepath => {
      t.equal(filepath, path.join(tempDir, 'log', '00000', '00', '00', '00000000000001.rlog'));

      return log.close();
    }).catch(err => {
      log.close();
      t.threw(err);
    });
  });

  suite.test('should replace FileLog', t => {
    t.plan(12 + 67 + 70 + 7 + 18);
    log = new FileLog(path.join(tempDir, 'log'), path.join(tempDir, 'snap'));
    t.type(log, FileLog);
    t.equal(log.requestIdTtl, raft.common.constants.DEFAULT_REQUEST_ID_TTL);
    return log.ready()
    .then(filelog => {
      t.equal(filelog, log);
      t.equal(log.logdir, path.join(tempDir, 'log'));
      t.equal(log.firstIndex, TOTAL_ENTRIES+1);
      t.equal(log.lastIndex, TOTAL_ENTRIES);
      t.equal(log.lastTerm, 1111);
      t.equal(log.indexFileCapacity, DEFAULT_CAPACITY);
      t.equal(log.snapshot.logIndex, TOTAL_ENTRIES);
      t.equal(log.snapshot.logTerm, 1111);
      t.equal(log.snapshot.dataSize, 0);
      t.equal(log.getFirstFreshIndex(), 1);

      return new Promise((resolve, reject) => {
        logentries = [];
        var hash = crypto.createHash('md5');
        var writer = log.createLogEntryWriteStream()
        .on('error', reject)
        .on('finish', () => {
          hash.once('readable', () => {
            digest = hash.read();
            resolve(writer.commit());
          });
          hash.end();
        });
        writer.write(new SnapshotChunk(Buffer.from('oh, snap!'), 0x1fffffff01, 0, 9, 77));
        randomEntryStream(100, 0x2000100001 - 0x1fffffff02, 99, 0x1fffffff02)
        .on('data', data => {
          logentries.push(data.length);
          hash.write(data);
        })
        .pipe(writer)
      })
    })
    .then(() => FileLog.readIndexFileNames(log.logdir))
    .then(files => {
      t.equal(files.length, 66);
      t.equal(files[0], path.join(tempDir, 'log', '00001', 'ff', 'ff', '00001fffffff02.rlog'));
      for(let n = 1, i = 0x100002000000000; i < 0x100002000100000; i += DEFAULT_CAPACITY) {
        t.equal(files[n++], path.join(tempDir, 'log', '00002', '00', '00',
                        i.toString(16).substr(1) + '.rlog'));
      }
      t.equal(files[65], path.join(tempDir, 'log', '00002', '00', '01', '00002000100000.rlog'));

      return FileLog.readIndexFileNames(log.logdir, (filepath) => {
        t.equal(filepath, path.join(tempDir, 'log', '00001', 'ff', 'ff', '00001fffffff02.rlog'));
      })
      .then(result => {
        t.equal(result, false);
        var n = 0;
        return FileLog.readIndexFileNames(log.logdir, (filepath) => {
          t.equal(filepath, files[n++]);
          return true;
        })
        .then(result => {
          t.equal(result, true);
          return n;
        });
      });
    })
    .then(n => {
      t.equal(n, 66);

      return log.findIndexFilePathOf(1);
    })
    .catch(err => {
      t.equal(err.code, 'ENOENT');
      return log.findIndexFilePathOf(0x00001fffffff01);
    })
    .then(filepath => {
      t.equal(filepath, undefined);
      return log.findIndexFilePathOf(0x00001fffffff02);
    })
    .then(filepath => {
      t.equal(filepath, path.join(tempDir, 'log', '00001', 'ff', 'ff', '00001fffffff02.rlog'));
      return log.findIndexFilePathOf(0x00001fffffffff);
    })
    .then(filepath => {
      t.equal(filepath, path.join(tempDir, 'log', '00001', 'ff', 'ff', '00001fffffff02.rlog'));
      return log.findIndexFilePathOf(0x00002000000000);
    })
    .then(filepath => {
      t.equal(filepath, path.join(tempDir, 'log', '00002', '00', '00', '00002000000000.rlog'));
      return log.findIndexFilePathOf(0x000020000fffff);
    })
    .then(filepath => {
      t.equal(filepath, path.join(tempDir, 'log', '00002', '00', '00', '0000' + (0x2000100000-DEFAULT_CAPACITY).toString(16) + '.rlog'));
      return log.findIndexFilePathOf(0x00002000100000);
    })
    .then(filepath => {
      t.equal(filepath, path.join(tempDir, 'log', '00002', '00', '01', '00002000100000.rlog'));

      t.equal(log.firstIndex, 0x1fffffff02);
      t.equal(log.lastIndex, 0x2000100000);
      t.equal(log.lastTerm, 99);
      t.equal(log.snapshot.logIndex, 0x1fffffff01);
      t.equal(log.snapshot.logTerm, 77);
      t.equal(log.snapshot.dataSize, 9);
      t.equal(log.getFirstFreshIndex(), 0x1fffffff02);
      return log.appendCheckpoint(100);
    })
    .then(() => {
      t.equal(log.firstIndex, 0x1fffffff02);
      t.equal(log.lastIndex, 0x2000100001);
      t.equal(log.lastTerm, 100);
      t.equal(log.getFirstFreshIndex(), 0x1fffffff02);

      return readdir(tempDir)
      .then(entries => {
        var logdirs = entries.filter(file => file.startsWith('log')).sort();
        t.equal(logdirs.length, 2);
        t.equal(logdirs[0], 'log');
        t.match(logdirs[1], /^log-\d{4}-\d\d-\d\d-\d{6}-\d{3}/);

        tempFiles.push(path.join(logdirs[1], '00000', '00', '00', '00000000000001.rlog'));

        var snaps = entries.filter(file => file.startsWith('snap')).sort();
        t.equal(snaps.length, 3);
        t.equal(snaps[0], 'snap');
        t.match(snaps[1], /^snap-\d{4}-\d\d-\d\d-\d{6}-\d{3}/);
        t.match(snaps[2], /^snap-\d{4}-\d\d-\d\d-\d{6}-\d{3}/);

        snaps.forEach(file => tempFiles.push(file));
      });
    })
    .then(() => {
      return log.close();
    }).catch(err => {
      log.close();
      t.threw(err);
    });
  });

  suite.test('should truncate FileLog', t => {
    t.plan(9 + (0x2000100001 - 0x1fffffff02) * 2 + 10);
    log = new FileLog(path.join(tempDir, 'log'), path.join(tempDir, 'snap'));
    t.type(log, FileLog);
    return log.ready()
    .then(filelog => {
      t.equal(filelog, log);
      t.equal(log.logdir, path.join(tempDir, 'log'));
      t.equal(log.firstIndex, 0x1fffffff02);
      t.equal(log.lastIndex, 0x2000100001);
      t.equal(log.lastTerm, 100);
      t.equal(log.snapshot.logIndex, 0x1fffffff01);
      t.equal(log.snapshot.logTerm, 77);
      t.equal(log.getFirstFreshIndex(), 0x1fffffff02);

      return new Promise((resolve, reject) => {
        var hash = crypto.createHash('md5')
            .once('readable', () => {
              try {
                t.equal(digest.equals(hash.read()), true);
              } catch(err) { return reject(err); }
              resolve();
            });
        var index = 0;
        var reader = log.createEntriesReadStream(0x1fffffff02, 0x2000100000, {maxChunkEntries: 10000, maxChunkSize: 128*1024})
        .on('error', reject)
        .on('end', () => hash.end())
        .on('data', entries => {
          try {
            entries.forEach(entry => {
              hash.write(entry);
              t.type(entry, Buffer);
              t.equal(entry.length, logentries[index++]);
            });
          } catch(err) { return reader.emit('error', err); }
        });
      })
    })
    .then(() => log.appendEntries([], 0x1fffffff02))
    .then(() => {
      t.equal(log.logdir, path.join(tempDir, 'log'));
      t.equal(log.firstIndex, 0x1fffffff02);
      t.equal(log.lastIndex, 0x1fffffff01);
      t.equal(log.lastTerm, 77);
      t.equal(log.snapshot.logIndex, 0x1fffffff01);
      t.equal(log.snapshot.logTerm, 77);
      t.equal(log.getFirstFreshIndex(), undefined);
      return FileLog.readIndexFileNames(log.logdir)
    })
    .then(files => {
      t.equal(files.length, 1);
      t.equal(files[0], path.join(tempDir, 'log', '00001', 'ff', 'ff', '00001fffffff02.rlog'));

      tempFiles.push(path.join('log', '00001', 'ff', 'ff', '00001fffffff02.rlog'));
    })
    .then(() => {
      return log.close();
    }).catch(err => {
      log.close();
      t.threw(err);
    });
  });

  suite.end();
});

function randomEntry(top, term, index) {
  var data = crypto.randomBytes(1 + (Math.random()*top>>>0));
  return LogEntry.build(genIdent(), LOG_ENTRY_TYPE_STATE, term, data, index);
}

function randomEntryStream(top, count, term, index) {
  return new Readable({
    objectMode: true,
    read() {
      if (count-- > 0) {
        this.push(randomEntry(top, term, index++));
      }
      else this.push(null);
    }
  });
}
