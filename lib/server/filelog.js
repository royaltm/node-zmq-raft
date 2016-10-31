/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const assert = require('assert')
    , path   = require('path')

const isArray = Array.isArray
    , isBuffer = Buffer.isBuffer
    , isEncoding = Buffer.isEncoding
    , now = Date.now
    , min = Math.min
    , max = Math.max
    , push = Array.prototype.push
    , MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER

const { readdir, openDir, closeDir, mkdirp, renameSyncDir} = require('../utils/fsutil');

const { assertConstantsDefined, defineConst, delay, regexpEscape } = require('../utils/helpers');

const { createRotateName } = require('../utils/filerotate');

const synchronize = require('../utils/synchronize');

const { createTempName, cleanupTempFiles } = require('../utils/tempfiles');

const ReadyEmitter = require('../common/readyemitter');
const IndexFile = require('../common/indexfile');
const SnapshotFile = require('../common/snapshotfile');
const LogStream = require('../server/logstream');

const { logPathComponents, logBaseName, logPath
      , INDEX_FILENAME_LENGTH, INDEX_FILE_EXT
      } = IndexFile;

const INDEX_BASENAME_LENGTH = INDEX_FILENAME_LENGTH - INDEX_FILE_EXT.length
    , ENTRY_CHECKPOINT_DATA = [0xc0]

const REQUESTDB_CLEANUP_INTERVAL = 10000;

const { REQUEST_LOG_ENTRY_OFFSET
      , REQUEST_LOG_ENTRY_LENGTH
      , REQUEST_LOG_ENTRY_BASE64_LENGTH
      , TYPE_LOG_ENTRY_OFFSET
      , TERM_LOG_ENTRY_OFFSET
      , LOG_ENTRY_HEADER_SIZE
      , LOG_ENTRY_TYPE_STATE
      , LOG_ENTRY_TYPE_CONFIG
      , LOG_ENTRY_TYPE_CHECKPOINT
      , hasRequestExpired
      , mixinReaders
      , build: buildLogEntry
      , LogEntry } = require('../common/log_entry');

assertConstantsDefined({
  INDEX_FILE_EXT
}, 'string');

assertConstantsDefined({
  INDEX_FILENAME_LENGTH
, REQUEST_LOG_ENTRY_OFFSET
, REQUEST_LOG_ENTRY_LENGTH
, REQUEST_LOG_ENTRY_BASE64_LENGTH
, TYPE_LOG_ENTRY_OFFSET
, TERM_LOG_ENTRY_OFFSET
, LOG_ENTRY_HEADER_SIZE
, LOG_ENTRY_TYPE_STATE
, LOG_ENTRY_TYPE_CONFIG
, LOG_ENTRY_TYPE_CHECKPOINT
}, 'number');

const zeroRequestBuf = Buffer.alloc(REQUEST_LOG_ENTRY_LENGTH, 0)
    , zeroRequestStr = zeroRequestBuf.toString('base64')
    , logEntryTypeStateBuf = Buffer.from([LOG_ENTRY_TYPE_STATE]);

const CACHE_INDEX_FILES_LIMIT_CAPACITY_LO = 50
    , CACHE_INDEX_FILES_LIMIT_CAPACITY_HI = 75

assert(CACHE_INDEX_FILES_LIMIT_CAPACITY_HI > CACHE_INDEX_FILES_LIMIT_CAPACITY_LO);

const debug = require('debug')('raft-filelog');

const indexFileCache$   =    Symbol("indexFileCache")
    , indexFileNames$   =    Symbol("indexFileNames")
    , lastIndexFile$    =    Symbol("lastIndexFile")
    , requestDb$        =    Symbol("requestDb")
    , rdbCleanInterval$ =    Symbol("rdbCleanInterval");

const isUInt = (v) => ('number' === typeof v && v % 1 === 0 && v >= 0 && v <= MAX_SAFE_INTEGER)
    , isValidTerm = isUInt
    , isValidIndex = isUInt;

/*

TODO: long lived request ids

Advanced log

state file

lastTerm 8 bytes LE + candidateId

slab.data msgpacked log entries [index,term,request,data]
slab.index log entry index

8 bytes first item index
4 bytes[index] offset in slab.data of message at first + 1 + index

Snapshot file

gzipped msgpacked version, config, lastIndex, lastTerm, ...state

config: [...servers]


Writing
=======

lock: a lock file for sanity, only one process/thread can update this

state: write to a new file then rename

log: 

writing to log:

if not opened file, find last file and open, or if not file found or the last no matches rolling criteria, create new one and index
if opened file but the index is before that file:
- delete current file and find previous one, repeat until found or every files deleted
if opened file and index is within this file and rolling criteria create new file
if opened file and index is within this file and no rolling criteria:
- truncate file if necessary
- write entry to file
- write entry offset to index

reading from log:

1. find index file
2. find index offset
3. read from file
4. next file goto 3

snapshoting log:

1. determine offset
2. create snapshot
3. replace current snapshot with a new snapshot
4. delete log before last snapshot offset + 1
4a. find first file
4b. if the whole file < first index delete file, next file, repeat


snapshot: just write

dirs:

state.data
state.data.tmpXXXXXX
snapshot.data
snapshot.data.tmpXXXXXXX
requests/ (hash map request -> index)

log/8765/43/21/87654321000000-87654321003FFF
log/8765/43/21/87654321FF0000-87654321FFFFFF

rolling criteria:
- N index entries boundary (N & 65536)
- max data size (<2^31)

log entry:

12 byte request id, 1 byte type, 7 byte LE term, the rest is log data (probably msgpacked or other)

type: 0 state
type: 1 cluster config
type: 2 checkpoint



start:

find first file/index
find last file/index (store last file)

read last X request ids to a map
reverse scan to find first 

*/

const msgpack = require('msgpack-lite');

class FileLog extends ReadyEmitter {

  constructor(logdir, snapshot, options) {
    super();
    options || (options = {});

    if (!logdir || 'string' !== typeof logdir) throw new TypeError("FileLog: first argument must be a directory name");
    defineConst(this, 'logdir', logdir);

    if (!snapshot || 'string' !== typeof snapshot) throw new TypeError("FileLog: second argument must be a path to the snapshot file");

    if (path.resolve(snapshot).startsWith(path.resolve(logdir))) {
      throw new TypeError("FileLog: snapshot must not be placed below the log directory");
    }

    this[indexFileCache$] = new IndexFileCache(this);
    this[indexFileNames$] = new Map();
    this[requestDb$] = new Map();

    this[rdbCleanInterval$] = null;

    debug('opening log: "%s"', logdir);

    mkdirp(logdir).then(() => Promise.all([
      findFirstOrLastIndexFile(logdir, 0),
      findFirstOrLastIndexFile(logdir, -1),
      new SnapshotFile(snapshot).ready().catch(err => {
        if (err.code !== 'ENOENT') throw err;
        /* snapshot file not found, so create a new one */
        return new SnapshotFile(snapshot, 0, 0, 0).ready();
      })
    ]))
    .then(([firstIndexPath, lastIndexPath, snapshot]) => {
      this.snapshot = snapshot;

      var firstIndexFile, lastIndexFile;
      if (firstIndexPath === undefined || lastIndexPath === undefined) {
        /* create new index file */
        firstIndexFile = lastIndexFile = new IndexFile(logdir, snapshot.logIndex + 1);
        this[indexFileCache$].add(lastIndexFile);
      }
      else {
        /* read indexes from first and last */
        lastIndexFile = new IndexFile(lastIndexPath);
        this[indexFileCache$].add(lastIndexFile);
        if (firstIndexPath === lastIndexPath) {
          firstIndexFile = lastIndexFile;
        }
        else {
          firstIndexFile = new IndexFile(firstIndexPath);
          this[indexFileCache$].add(firstIndexFile);
        }
      }
      return Promise.all([firstIndexFile.ready(), lastIndexFile.ready()]);
    })
    .then(([firstIndexFile, lastIndexFile]) => {
      /* sanity check */
      if (this.snapshot.logIndex + 1 < firstIndexFile.firstAllowedIndex) {
        throw new Error("snapshot last index does not precede immediately first log index");
      }
      /* set first index */
      this.firstIndex = max(firstIndexFile.firstAllowedIndex, this.snapshot.logIndex + 1);
      return this._findRealLastIndexFile(firstIndexFile, lastIndexFile)
      .then(lastIndexFile => {
        this.lastIndex = lastIndexFile.nextIndex - 1;
        if (this.snapshot.logIndex >= this.lastIndex && this.lastIndex >= this.firstIndex) {
          throw new Error("snapshot last index covers entire log: " + this.snapshot.logIndex); // TODO: discard log here
        }
        this[lastIndexFile$] = lastIndexFile;
        return this.termAt(this.lastIndex)
              .then(term => this.lastTerm = term);
      });
    })
    .then(() => this._readRequestsFromLogs())
    .then(() => {
      debug('first index: %s, last index: %s, last term: %s', this.firstIndex, this.lastIndex, this.lastTerm);

      cleanupTempFiles(this.snapshot.filename, debug).catch(err => debug('temporary files cleanup ERROR: %s', err));

      this[Symbol.for('setReady')]();
    })
    .catch(err => this.error(err));
  }

  close() {
    var snapshot = this.snapshot;
    return synchronize(this, () => {
      if (!snapshot) return;
      clearInterval(this[rdbCleanInterval$]);
      this[rdbCleanInterval$] = null;
      // TODO: need to wait for closing all index files? yes parhaps
      var promises = [];
      for(let indexFile of this[indexFileCache$].values()) promises.push(indexFile.close());
      this[indexFileCache$].clear();
      this[indexFileNames$].clear();
      this[indexFileCache$] = null;
      this[indexFileNames$] = null;
      this[lastIndexFile$] = null;
      this.snapshot = null;
      promises.push(snapshot.close());
      return Promise.all(promises);
    });
  }

  getRid(requestId) {
    if ('string' === typeof requestId && requestId.length === REQUEST_LOG_ENTRY_BASE64_LENGTH) {
      return this[requestDb$].get(requestId);
    }
    else if (isBuffer(requestId) && requestId.length === REQUEST_LOG_ENTRY_LENGTH) {
      return this[requestDb$].get(requestId.toString('base64'));
    }
    throw new TypeError("FileLog.getRid: requestId must be a 16 characters base64 string or a 12 byte buffer");
  }

  appendCheckpoint(term) {
    if (!isValidTerm(term)) return Promise.reject(new Error("FileLog.appendCheckpoint: term is invalid"));

    return synchronize(this, () => {
      var entry = this._checkpointLogEntry;
      if (entry === undefined) {
        entry = this._checkpointLogEntry = buildLogEntry(null, LOG_ENTRY_TYPE_CHECKPOINT, term, ENTRY_CHECKPOINT_DATA);
      }
      else entry.writeEntryTerm(term);

      debug('applying checkpoint with term: %s', term);

      return this._lastIndexFile()
      .then(indexFile => synchronize(indexFile, () => this._append(indexFile, entry, term)));
    });
  }

  appendState(requestId, term, data) {
    if (!isValidTerm(term)) return Promise.reject(new Error("FileLog.appendCheckpoint: term is invalid"));

    return synchronize(this, () => {
      var requestKey;
      if ('string' === typeof requestId && requestId.length === REQUEST_LOG_ENTRY_BASE64_LENGTH) {
        requestKey = requestId;
        requestId = Buffer.from(requestId, 'base64');
      } else if (isBuffer(requestId) && requestId.length === REQUEST_LOG_ENTRY_LENGTH) {
        requestKey = requestId.toString('base64');
      } else {
        throw new TypeError("FileLog.appendState: requestId must be a 16 characters base64 string or a 12 byte buffer");
      }

      return this._lastIndexFile()
      .then(indexFile => synchronize(indexFile, () => {
        var termBuf = indexFile._termLogEntryBuf || (indexFile._termLogEntryBuf = Buffer.allocUnsafe(7));
        termBuf.writeUIntLE(term, 0, 7, true);

        debug('applying state entry: (%s) with term: %s', data.length, term);

        return this._append(indexFile, [[requestId, logEntryTypeStateBuf, termBuf, data]], term)
        .then(lastIndex => {
          this[requestDb$].set(requestKey, lastIndex);
          return lastIndex;
        });
      }));

    });
  }

  _append(indexFile, entry, term) {
    return indexFile.append(entry).then(([numUnwritten, nextEntry]) => {
      this[lastIndexFile$] = indexFile;
      if (numUnwritten === 0) {
        this.lastTerm = term;
        return this.lastIndex = nextEntry - 1;
      }
      else {
        return this._createNewIndexFile(indexFile)
          .then(indexFile => synchronize(indexFile, () => this._append(indexFile, entry, term)));
      }
    });
  }

  appendEntries(entries, index) {
    if (index !== undefined && !isValidIndex(index)) throw new Error("FileLog.appendEntries: index is invalid");
    if (!isArray(entries) || !entries.every(b => isBuffer(b) && b.length > LOG_ENTRY_HEADER_SIZE)) {
      return Promise.reject(new Error("FileLog.appendEntries: entries are invalid"));
    }
    return synchronize(this, () => {
      if (index === undefined) index = this.lastIndex + 1;

      return this._truncate(index).then(() => {
        const firstIndex = index
            , numEntries = entries.length;

        if (numEntries === 0) return; /* already truncated, no-op */

        const rdb = this[requestDb$];

        const write = (indexFile, index, entries) => synchronize(indexFile, () => indexFile.writev(entries, index)
        .then(([numUnwritten, nextEntry]) => {
          var lastWritten = nextEntry - index - 1
            , entry, req, i;
          this[lastIndexFile$] = indexFile;
          if (lastWritten >= 0) {
            for(i = 0; i <= lastWritten; ++i) {
              entry = entries[i];
              if (!hasRequestExpired(entry, REQUEST_LOG_ENTRY_OFFSET)) {
                req = entry.toString('base64', REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_LENGTH);
                rdb.set(req, index + i);
              }
            }
            this.lastIndex = nextEntry - 1;
            this.lastTerm = entries[lastWritten].readUIntLE(TERM_LOG_ENTRY_OFFSET, 7, true);
          }
          if (numUnwritten !== 0) {
            return this._createNewIndexFile(indexFile)
            .then(indexFile => write(indexFile, nextEntry, entries.slice(-numUnwritten)));
          }
          else debug('applied log (%s) indexes: %s - %s last term: %s', numEntries, firstIndex, this.lastIndex, this.lastTerm);
        }));

        return this._lastIndexFile().then(indexFile => write(indexFile, index, entries));
      });
    });
  }

  _truncate(index) {
    var nextIndex = this.lastIndex + 1;
    if (index < this.firstIndex || index > nextIndex) return Promise.reject(new Error("FileLog.truncate: index out of index range"));
    if (index === nextIndex) return Promise.resolve(); /* no-op */

    var lastIndex = index - 1;

    debug('truncating log before %s', index);

    var truncate = (lastIndexFile) => {
      if (lastIndexFile.allowed(lastIndex)) {
        return synchronize(lastIndexFile, () => readTermAt(lastIndexFile, lastIndex).then(term => {
          this.lastIndex = lastIndex;
          this.lastTerm = term;
          return lastIndexFile.truncate(index);
        }));
      }
      else if (index === this.firstIndex && index === lastIndexFile.firstAllowedIndex) {
        return synchronize(lastIndexFile, () => {
          this.lastIndex = lastIndex;
          this.lastTerm = this.snapshot.logTerm;
          return lastIndexFile.truncate(index);
        });
      }
      else {
        return this._indexFileOf(lastIndexFile.firstAllowedIndex - 1)
        .then(prevIndexFile => synchronize(prevIndexFile,
                                () => readTermAt(prevIndexFile, prevIndexFile.lastAllowedIndex))
          .then(term => synchronize(lastIndexFile, () => {
            this.lastIndex = prevIndexFile.lastAllowedIndex;
            this.lastTerm = term;
            this[lastIndexFile$] = prevIndexFile;
            debug('deleting log file: %s', lastIndexFile);
            lastIndexFile._cacheTombstone = true;
            return lastIndexFile.destroy()
                    .then(() => {
                      this[indexFileCache$].delete(lastIndexFile.basename);
                      this._pruneFileNamesCache(lastIndexFile.basename);
                      return prevIndexFile;
                    });
          }))
        ).then(truncate);
      }
    };

    return this._lastIndexFile().then(truncate);
  }

  getEntry(index, buffer) {
    return this._indexFileOf(index)
    .then(indexFile => synchronize(indexFile, () => {
      if (isBuffer(buffer) && buffer.length >= indexFile.getByteSize(index, 1)) {
        return indexFile.readb(index, 1, buffer, 0).then(length => buffer.slice(0, length));
      }
      else return indexFile.read(index, 1);
    }));
  }

  getEntries(firstIndex, lastIndex) {
    if (lastIndex > this.lastIndex) return Promise.reject(new Error("FileLog.getEntries: lastIndex too large"));
    const result = [];
    if (lastIndex < firstIndex) return Promise.resolve(result);

    const next = (index) => this._indexFileOf(index).then(indexFile => synchronize(indexFile, () => {
      const maxIndex = min(indexFile.lastAllowedIndex, lastIndex);
      return indexFile.readv(index, maxIndex - index + 1);
    }))
    .then(entries => {
      push.apply(result, entries);
      const nextIndex = index + entries.length;
      return (nextIndex <= lastIndex) ? next(nextIndex) : result;
    });

    return next(firstIndex);
  }

  readEntries(firstIndex, lastIndex, buffer) {
    var offset = 0, length = buffer.length;
    if (lastIndex > this.lastIndex) return Promise.reject(new Error("FileLog.getEntries: lastIndex too large"));
    const result = [];
    if (lastIndex < firstIndex) return Promise.resolve(result);

    const next = (index) => this._indexFileOf(index).then(indexFile => synchronize(indexFile, () => {
      const maxIndex = min(indexFile.lastAllowedIndex, lastIndex)
          , count = indexFile.countEntriesFitSize(index, maxIndex - index + 1, length - offset);

      if (count !== 0) return indexFile.readb(index, count, buffer, offset)
                        .then(size => {
                          assert(size > 0);
                          const entries = indexFile.splitb(index, count, buffer, offset);
                          push.apply(result, entries);
                          offset += size;
                          return index + count;
                        });
      else return lastIndex + 1;
    }))
    .then(nextIndex => {
      if (nextIndex <= lastIndex) {
        return next(nextIndex);
      }
      else if (result.length === 0) {
        /* nothing fit into the buffer */
        debug('won\'t fit index: %s in %s', firstIndex, buffer.length);
        return this.getEntry(firstIndex).then(entry => [entry]);
      }
      else return result;
    });

    return next(firstIndex);
  }

  /* streams are not synchronized to allow parallell reads,
      you are responsible to ensure that no log will be truncated below the lastIndex while streaming,
      raft specification never allow to truncate commited entries so as long as we stream only
      commited ones everything should be in order */
  createEntriesReadStream(firstIndex, lastIndex, options) {
    if (!isValidIndex(firstIndex) || firstIndex < this.firstIndex || firstIndex > this.lastIndex) {
      throw new TypeError("FileLog.streamEntries: firstIndex must be a valid index");
    }
    if (!isValidIndex(lastIndex) || lastIndex < this.firstIndex || lastIndex > this.lastIndex) {
      throw new TypeError("FileLog.streamEntries: firstIndex must be a valid index");
    }
    return new LogStream(this, firstIndex, lastIndex, options);
  }

  termAt(index) {
    if (index < this.firstIndex || index > this.lastIndex) {
      if (index === this.snapshot.logIndex) return Promise.resolve(this.snapshot.logTerm);
      return Promise.resolve();
    }
    return this._indexFileOf(index)
    .then(indexFile => synchronize(indexFile, () => readTermAt(indexFile, index)));
  }

  createTmpSnapshot(index, term, dataSize) {
    return new SnapshotFile(createTempName(this.snapshot.filename), index, term, dataSize);
  }

  installSnapshot(snapshot) {
    if (snapshot instanceof SnapshotFile) {
      return snapshot.ready().then(() => synchronize(this, () => {
        var currentSnapshot = this.snapshot;
        if (snapshot === currentSnapshot) return;
        debug('installing snapshot index: %s term: %s dataSize: %s', snapshot.logIndex, snapshot.logTerm, snapshot.dataSize);
        debug('replacing snapshot index: %s term: %s dataSize: %s', currentSnapshot.logIndex, currentSnapshot.logTerm, currentSnapshot.dataSize);
        return currentSnapshot.close().then(() => snapshot.replace(currentSnapshot.filename))
        /* TODO: this is the moment when we don't have snapshot file and thing may go south in some circumstances
           need more insight, proposed solution: synchronize(currentSnapshot) */
        .then(() => this.termAt(snapshot.logIndex).then(term => {
          this.snapshot = snapshot;
          if (snapshot.logTerm === term) {
            /* just mark */
            this.firstIndex = snapshot.logIndex + 1;
            /* TODO: wipe out obsolete log files in background */
          }
          else {
            /* discard the entire log (rename logdir, create new log dir, new files, new caches etc) */
            return createNewLogDirectory.call(this, snapshot);
          }
        }));
      }));
    }
    else throw new TypeError("FileLog.installSnapshot: snapshot must be an instance of SnapshotFile");
  }

  // firstIndexOfTerm(term) {

  // }

  _createNewIndexFile(lastIndexFile) {
    var index = lastIndexFile.lastAllowedIndex + 1;
    var basename = logBaseName(index);
    lastIndexFile = this[indexFileCache$].get(basename);
    if (!lastIndexFile) {
      debug('creating new index file: %s', index);
      lastIndexFile = new IndexFile(this.logdir, index);
      this[indexFileCache$].add(lastIndexFile);
      this._pruneFileNamesCache(basename);
    }
    return lastIndexFile.ready();
  }

  _lastIndexFile() {
    return this[lastIndexFile$].ready();
  }

  _indexFileOf(index) {
    var lastIndexFile = this[lastIndexFile$];
    if (lastIndexFile.isReady && lastIndexFile.includes(index)) return lastIndexFile.ready();
    var found = (basename) => {
      if (basename !== undefined && index <= this.lastIndex) {
        var indexFile = this[indexFileCache$].get(basename);
        if (indexFile === undefined || indexFile.isClosed) {
          debug('opening index file: %s', basename);
          indexFile = new IndexFile(logPath(this.logdir, basename));
          this[indexFileCache$].add(indexFile);
        }
        else if (indexFile._cacheTombstone) {
          debug("index file with tombstone: %s", basename);
          /* try again when old indexFile is closed */
          return synchronize(indexFile, () => found(basename));
        }
        return indexFile.ready().then(indexFile => {
          if (!indexFile.includes(index)) {
            throw new Error("FileLog: could not find index file for: " + index.toString(16));
          }
          return indexFile;
        });
      }
      throw new Error("FileLog: could not find index file for: " + index.toString(16));
    };

    return this._indexFileNameOf(index).then(found);
  }

  /* refresh indexFileNames when files destroyed/new created */
  _pruneFileNamesCache(index) {
    var basename = logBaseName(index);
    var prefix = basename.substr(0, 8);
    this[indexFileNames$].delete(prefix);
  }

  _indexFileNameOf(index) {
    if (index < this.firstIndex || index > this.lastIndex) return Promise.resolve();
    var basename = logBaseName(index);
    var prefix = basename.substr(0, 8);
    var indexFileNames = this[indexFileNames$];
    var entries = indexFileNames.get(prefix);
    if (entries === undefined) {
      debug('no names for: %s, reading dir', prefix);
      entries = readdir(path.dirname(logPath(this.logdir, basename)))
      .then(entries => entries.filter(file => file.length === INDEX_FILENAME_LENGTH && file.endsWith(INDEX_FILE_EXT))
                     .map(file => file.substr(0, INDEX_BASENAME_LENGTH))
                     .sort()
      );
      indexFileNames.set(prefix, entries);
    }
    return entries.then(entries => bSearch(entries, basename));
  }

  _findRealLastIndexFile(firstIndexFile, lastIndexFile) {
    if (firstIndexFile === lastIndexFile || lastIndexFile.nextIndex > lastIndexFile.firstAllowedIndex) {
      this[indexFileNames$].clear();
      return Promise.resolve(lastIndexFile);
    }
    else {
      debug('last index file: %s is empty, scanning for the last entry', lastIndexFile);
      this[indexFileCache$].delete(lastIndexFile.basename);
      this.lastIndex = lastIndexFile.nextIndex - 1;
      this[lastIndexFile$] = lastIndexFile;
      return this._indexFileOf(this.lastIndex)
        .then(indexFile => lastIndexFile.destroy()
                        .then(() => this._findRealLastIndexFile(firstIndexFile, indexFile)));
    }
  }

  _readRequestsFromLogs() {
    debug("reading request ids from index files");
    var lastIndexFile = this[lastIndexFile$];
    var rdb = this[requestDb$];
    rdb.clear();
    var temprdb = [];
    var numExpired = 0;
    var requestId = Buffer.allocUnsafe(REQUEST_LOG_ENTRY_LENGTH);
    var start = Date.now();
    var read = (indexFile, index) => {
      if (indexFile.includes(index)) {
        return indexFile.readSlice(index, REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_LENGTH, requestId).then(() => {
          if (!requestId.equals(zeroRequestBuf)) {
            if (hasRequestExpired(requestId)) {
              if (++numExpired > indexFile.capacity) {
                /* assume log entries before have already expired request ids */
                debug("no more fresh entries");
                return;
              }
            }
            else {
              temprdb.push(index, requestId.toString('base64'));
              // rdb.set(requestId.toString('base64'), index);
            }
          }
          // else {
          //   debug('ZERO: %s', index);
          // }
          return read(indexFile, index - 1);
        });
      }
      else if (index >= this.firstIndex && index < indexFile.firstAllowedIndex) {
        return this._indexFileOf(index).then(indexFile => read(indexFile, index));
      }
    };

    return read(lastIndexFile, this.lastIndex).then(() => {
      /* copy temprdb to rdb backwards, so oldest entries go first */
      for(let i = temprdb.length; i-- > 0; ) {
        let requestKey = temprdb[i--], index = temprdb[i];
        rdb.set(requestKey, index);
      }
      temprdb.length = 0;
      temprdb = null;
      debug("read: %s requests in %s seconds", rdb.size, ((Date.now() - start) / 1000).toFixed(1));
      /* set-up rdb cleanup routine */
      this[rdbCleanInterval$] = setInterval(() => {
        var rdb = this[requestDb$];
        for(var req of rdb.keys()) {
          if (hasRequestExpired(req)) {
            debug("rdb cleanup: forget about %s", req);
            rdb.delete(req);
          }
          else break;
        }
      }, REQUESTDB_CLEANUP_INTERVAL);
    });
  }

  decodeRequestId(requestId) {
    return requestId.toString('base64');
  }

}


mixinReaders(FileLog.prototype);

module.exports = exports = FileLog;

/* find closest entry (but equal or smaller than searched) */
function bSearch(entries, searched) {
  var lo = 0, hi = entries.length - 1;
  if (hi < 0) return;
  var mid = (lo + hi + 1) >> 1;
  var found = entries[mid];
  while(lo < hi) {
    if (found > searched) hi = mid - 1; else lo = mid;
    mid = (lo + hi + 1) >> 1;
    found = entries[mid];
  }
  if (searched >= found) return found;
}

function readTermAt(indexFile, index) {
  var buffer = indexFile._termLogEntryBuf || (indexFile._termLogEntryBuf = Buffer.allocUnsafe(7));
  return indexFile.readSlice(index, TERM_LOG_ENTRY_OFFSET, TERM_LOG_ENTRY_OFFSET + 7, buffer)
       .then(() => buffer.readUIntLE(0, 7, true))
}

/*
  - rename log to log-...
  - create new empty log directory and file
  - replace all caches (names and index files)
  - set this.firstIndex this.lastIndex this.lastTerm
*/
function createNewLogDirectory(snapshot) {
  const logdir = this.logdir;
  const oldIndexFileCache = this[indexFileCache$];
  const indexFileCache = this[indexFileCache$] = new IndexFileCache(this);
  this[lastIndexFile$] = null;

  return oldIndexFileCache.close().then(() => {
    return renameSyncDir(logdir, createRotateName(logdir))
    .then(() => mkdirp(logdir))
    .then(created => {
      if (!created) throw new Error("FileLog: can't create new file log - logdir still exists");
      const indexFile = new IndexFile(logdir, snapshot.logIndex + 1);
      indexFileCache.add(indexFile);
      this[indexFileNames$].clear();
      this[lastIndexFile$] = indexFile;
      this.firstIndex = snapshot.logIndex + 1;
      this.lastIndex = snapshot.logIndex;
      this.lastTerm = snapshot.logTerm;
    });
  });
}

/**
 * resolves to first or last index file
 *
 * @param {string} logdir
 * @param {number} which - positive find first, negative find last
 * @return {Promise}
**/
function findFirstOrLastIndexFile(logdir, which) {
  which |= 0;
  var inc = which < 0 ? -1 : 1;
  var patterns = logPathComponents(logdir, 0).slice(1)
      .map(name => new RegExp('^' + regexpEscape(name).replace(/0/g,'[0-9a-f]') + '$'));

  var scandir = (dir, depth) => {
    return readdir(dir)
      .then(entries => {
        var re = patterns[depth++];
        entries = entries.filter(name => re.test(name)).sort();

        var iterate = (index) => {
          var name = entries[index];
          if (name === undefined) return;
          if (depth === patterns.length) return path.join(dir, name);
          return scandir(path.join(dir, name), depth).then(filepath => {
            /* iterate through intermediate directory entries to find first/last file
               directories might be empty if log files has been deleted but directories wasn't */
            return filepath !== undefined ? filepath : iterate(index + inc);
          });
        };

        return iterate(inc === -1 ? entries.length - 1 : 0);
      });
  };

  return scandir(logdir, 0);
}


class IndexFileCache extends Map {
  constructor(fileLog) {
    if (!(fileLog instanceof FileLog)) throw TypeError("IndexFileCache requires instance of FileLog");
    super();
    this.fileLog = fileLog;
  }

  close() {
    var files = Array.from(this.values());
    this.clear();
    return IndexFileCache.closeAll(files);
  }

  static closeAll(files) {
    var promises = [];
    var unclosed = [];
    for(let indexFile of files) {
      if (indexFile._cacheTombstone) { /* will be closed anyway */
        promises.push(synchronize(indexFile));
      }
      else {
        if (indexFile._cacheRefs) {
          unclosed.push(indexFile);
        } else {
          promises.push(synchronize(indexFile, () => indexFile.ready().then(() => indexFile.close())));
        }
      }
    }
    if (unclosed.length !== 0) {
      promises.push(delay(1000).then(() => IndexFileCache.closeAll(unclosed)));
    }
    return Promise.all(promises);
  }

  get(basename) {
    var entry = super.get(basename);
    if (entry !== undefined) {
      /* move to the end */
      this.delete(basename);
      this.set(basename, entry);
      return entry;
    }
  }

  add(indexFile) {
    if (indexFile._cacheRefs === undefined) indexFile._cacheRefs = 0;
    this.delete(indexFile.basename);
    this.set(indexFile.basename, indexFile);
    if (this.size >= CACHE_INDEX_FILES_LIMIT_CAPACITY_HI) this.purge();
  }

  purge() {
    debug('purging index file cache');
    var lastIndexFile = this.fileLog[lastIndexFile$];
    var size = this.size;
    for(let indexFile of this.values()) {
      if (size <= CACHE_INDEX_FILES_LIMIT_CAPACITY_LO) break;
      // if (indexFile._cacheRefs) console.log('skip: %s indexFile._cacheRefs: %s', indexFile, indexFile._cacheRefs);
      if (indexFile._cacheTombstone || indexFile._cacheRefs || indexFile === lastIndexFile) {
        --size;
      }
      else if (indexFile.isReady) {
        --size;
        debug('will purge: %s', indexFile);
        indexFile._cacheTombstone = true;
        synchronize(indexFile, () => indexFile.ready().then(() => indexFile.close())
        .then(()=> {
          if (super.get(indexFile.basename) === indexFile) {
            debug('will remove cached: %s', indexFile);
            this.delete(indexFile.basename);
          } else debug('already removed cached: %s', indexFile);
        }));
      }
    }
  }
}

/*
var ben=require('ben')
var genIdent = require('./raft/id').genIdent;
var FileLog = require('./raft/filelog');
var log = new FileLog('data/log','data/snap');log.ready().then(r=>console.log('ret: %s', r), console.error)
log.installSnapshot(log.createTmpSnapshot(10, 10, 10000)).then(r=>console.log('ret: %s', r), console.error)

var persistence = new RaftPersistence('tmp/01/raft.pers', []);persistence.ready().then(console.log,console.warn);
persistence.update({currentTerm: 10}).then(console.log,console.warn);


function entries(num,term) {
  var res = [];
  while(num-- > 0) {
    var data = crypto.randomBytes(22 + (Math.random()*5000>>>0));
    var req = genIdent(data, 0);
    data[12] = 0;
    data.writeUIntLE(term||log.lastTerm, 13, 7, true);
    res.push(data);
  }
  return res;
}
function sizer(a) {for(var s=0,i=a.length;i-->0;s+=a[i].length); return s;}
var my=entries(100000, 1),i=0;sizer(my);
ben.async(1, cb=>log.appendEntries(my, 1).then(cb,console.error),ms=>console.log('ms: %s',ms));

strm.on('data', e => {console.log('%s: %d, %s: %s', id, i, e.length,sizer(e));e.forEach(b=>assert(b.equals(my[i++])));});
strm.on('data', e => {console.log('%d, %s: %s %s', i, e.length,sizer(e),e.map(b=>b.length));e.forEach(b=>assert(b.equals(my[i++])));});

function wow(id,me,ms) {
  var i=0;strm=log.createEntriesReadStream(log.firstIndex, 0xfffe, {maxChunkEntries: me, maxChunkSize: ms});
  strm.on('error', console.error);
  strm.on('end', ()=>console.log('EOF: %s', id));
  strm.on('data', e => {e.forEach(b=>assert(b.equals(my[i++])));});
  return strm;
}
function delay(x) { return new Promise((r,e)=>setTimeout(r,x)); }
Promise.resolve(wow(':O')).then(s=>delay(50)).then(()=>ben.async(1, cb=>log.appendEntries(my, 0xffff).then(cb,console.error),ms=>console.log('ms: %s',ms)));
Promise.resolve(wow(':O')).then(s=>{ delay(Math.random()*150>>>0).then(()=>{console.log('cancel');s.cancel();})});
Promise.resolve(wow(':O')).then(s=>{ delay(90).then(()=>{console.log('cancel');s._if.close();})});
wow(':)',1);delay(250).then(()=>{wow(':(');return delay(500)}).then(()=>{wow(':|',10); return delay(300)}).then(()=>{wow(':~',5); return delay(250)}).then(()=>{wow(':<',0,500); return delay(250)}).then(()=>{wow(':$',25); return delay(250)}).then(()=>wow(':*',1))
wow(':)',1);delay(250).then(()=>{wow(':(');return delay(500)}).then(()=>{wow(':|',10); return delay(300)}).then(()=>{wow(':~',5); return delay(250)}).then(()=>{wow(':<',0,500); return delay(250)}).then(()=>{wow(':$',25); return delay(250)}).then(()=>wow(':*',1))
wow(':)',1);delay(250).then(()=>{wow(':(');return delay(500)}).then(()=>{wow(':|',10); return delay(300)}).then(()=>{wow(':~',5); return delay(250)}).then(()=>{wow(':<',0,500); return delay(250)}).then(()=>{wow(':$',25); return delay(250)}).then(()=>wow(':*',1))

log.termAt(65538).then(console.log,console.error)
log.appendEntries([],100001).then(console.log,console.error)
log.appendEntries(entries(32768,2)).then(console.log,console.error)
ben.async(100,cb=>log.appendEntries(entries(1000,4)).then(cb,console.error),ms=>console.log('ms: %s',ms))

log.appendCheckpoint(1).then(console.log,console.error)
log.appendEntries([Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,2,3,0,0,0,0,0,0,0xc0]),Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,2,3,0,0,0,0,0,0,0xc0]),Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,2,3,0,0,0,0,0,0,0xc0])]).then(console.log,console.error)
log.appendEntries([Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,2,5,0,0,0,0,0,0,0xc0]),Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,2,6,0,0,0,0,0,0,0xc0]),Buffer.from([0,0,0,0,0,0,0,0,0,0,0,0,2,7,0,0,0,0,0,0,0xc0])], 65536).then(console.log,console.error)

*/