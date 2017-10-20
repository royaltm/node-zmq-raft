/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const assert = require('assert')
    , path   = require('path')
    , { watch, constants: { R_OK, W_OK } } = require('fs')

const isArray = Array.isArray
    , isBuffer = Buffer.isBuffer
    , isEncoding = Buffer.isEncoding
    , now = Date.now
    , min = Math.min
    , max = Math.max
    , push = Array.prototype.push
    , MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER

const { access, readdir, openDir, closeDir, mkdirp, renameSyncDir} = require('../utils/fsutil');

const { assertConstantsDefined, defineConst, delay, regexpEscape } = require('../utils/helpers');

const { createRotateName } = require('../utils/filerotate');

const synchronize = require('../utils/synchronize');
const { exclusive: lockExclusive, shared: lockShared } = require('../utils/lock');

const { createTempName, cleanupTempFiles } = require('../utils/tempfiles');

const ReadyEmitter = require('../common/readyemitter');
const StateMachineWriter = require('../common/state_machine_writer');
const IndexFile = require('../common/indexfile');
const SnapshotFile = require('../common/snapshotfile');
const LogStream = require('../server/logstream');

const { logPathComponents, logBaseName, logPath
      , INDEX_FILENAME_LENGTH
      , INDEX_FILE_EXT
      , INDEX_PATH_PREFIX_LENGTH
      } = IndexFile;

const INDEX_BASENAME_LENGTH = INDEX_FILENAME_LENGTH - INDEX_FILE_EXT.length
    , ENTRY_CHECKPOINT_DATA = [0xc0]

const FEED_STATE_NUM_ENTRIES_TRESHOLD = 2;

const REQUESTDB_CLEANUP_INTERVAL = 10000;

const INSTALL_SNAPSHOT_WATCHER_COOLDOWN_INTERVAL = 10000;

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
      , readers: { readTypeOf }
      , mixinReaders
      , build: buildLogEntry
      , LogEntry } = require('../common/log_entry');

const REQUEST_LOG_ENTRY_END = REQUEST_LOG_ENTRY_OFFSET + REQUEST_LOG_ENTRY_LENGTH;

assertConstantsDefined({
  INDEX_FILE_EXT
}, 'string');

assertConstantsDefined({
  INDEX_FILENAME_LENGTH
, INDEX_PATH_PREFIX_LENGTH
, REQUEST_LOG_ENTRY_OFFSET
, REQUEST_LOG_ENTRY_LENGTH
, REQUEST_LOG_ENTRY_BASE64_LENGTH
, REQUEST_LOG_ENTRY_END
, TYPE_LOG_ENTRY_OFFSET
, TERM_LOG_ENTRY_OFFSET
, LOG_ENTRY_HEADER_SIZE
, LOG_ENTRY_TYPE_STATE
, LOG_ENTRY_TYPE_CONFIG
, LOG_ENTRY_TYPE_CHECKPOINT
}, 'number');

const zeroRequestBuf = Buffer.alloc(REQUEST_LOG_ENTRY_LENGTH, 0)
    , logEntryTypeBuffers = {
      [LOG_ENTRY_TYPE_STATE]:      Buffer.from([LOG_ENTRY_TYPE_STATE])
    , [LOG_ENTRY_TYPE_CONFIG]:     Buffer.from([LOG_ENTRY_TYPE_CONFIG])
    , [LOG_ENTRY_TYPE_CHECKPOINT]: Buffer.from([LOG_ENTRY_TYPE_CHECKPOINT])
  };

const CACHE_INDEX_FILES_LIMIT_CAPACITY_LO = 50
    , CACHE_INDEX_FILES_LIMIT_CAPACITY_HI = 75

assert(CACHE_INDEX_FILES_LIMIT_CAPACITY_HI > CACHE_INDEX_FILES_LIMIT_CAPACITY_LO);

const debug = require('debug')('zmq-raft:filelog');

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

FileLog
=======

only one process/thread can update the log

type: 0 state
type: 1 cluster config
type: 2 checkpoint

log entry format

offs. content

  0 | 12-bytes request id
 12 | 1 byte entry type
 13 | 7-byte LSB unsigned long term
 20 | data

snapshoting log:

1. determine offset
2. create snapshot
3. replace current snapshot with a new snapshot
4. delete log before last snapshot offset + 1
4a. find first file
4b. if the whole file < first index delete file, next file, repeat


dirs:

log/8765/43/21/87654321000000-87654321003FFF
log/8765/43/21/87654321FF0000-87654321FFFFFF

rolling criteria:
- N index entries boundary (N & 65536)
- max data size (<2^31)

*/

class FileLog extends ReadyEmitter {
  /**
   * creates a new FileLog instance
   *
   * @param {string} logdir - a path to the directory where the root of the log file structure will be created
   * @param {string} snapshot - a path to the snapshot file (existing or to be created)
   * @param {boolean} [readOnly] - pass true to open in read only mode
   * @return {FileLog}
  **/
  constructor(logdir, snapshot, readOnly) {
    super();

    if (!logdir || 'string' !== typeof logdir) throw new TypeError("FileLog: first argument must be a directory name");
    defineConst(this, 'logdir', logdir);

    if (!snapshot || 'string' !== typeof snapshot) throw new TypeError("FileLog: second argument must be a path to the snapshot file");

    if (path.resolve(snapshot).startsWith(path.resolve(logdir))) {
      throw new TypeError("FileLog: snapshot must not be placed in the log directory");
    }

    this[indexFileCache$] = new IndexFileCache(this);
    this[indexFileNames$] = new Map();
    this[requestDb$] = new Map();

    this[rdbCleanInterval$] = null;

    initializeLogFile.call(this, logdir, snapshot, readOnly)
    .then(() => {
      debug('first index: %s, last index: %s, last term: %s', this.firstIndex, this.lastIndex, this.lastTerm);
      this[Symbol.for('setReady')]();
    })
    .catch(err => this.error(err));
  }

  /**
   * closes FileLog instance
   *
   * @return {Promise}
  **/
  close() {
    var snapshot = this.snapshot;
    return synchronize(this, () => {
      if (!snapshot) return;
      debug('closing');
      if (this.installSnapshotWatcher) {
        this.installSnapshotWatcher.close();
        this.installSnapshotWatcher = null;
      }
      clearInterval(this[rdbCleanInterval$]);
      this[rdbCleanInterval$] = null;
      this[lastIndexFile$] = null;
      var promises = [this[indexFileCache$].close(), snapshot.close()];
      this[indexFileNames$].clear();
      this[indexFileCache$] = null;
      this[indexFileNames$] = null;
      this.snapshot = null;
      return Promise.all(promises);
    });
  }

  /**
   * returns log entry index for the first (the oldest) update request id
   * that is still remembered (and probably still fresh)
   *
   * this can be helpfull to determine up to which index it's safe to prune log entry files
   * after installing log compaction snapshot
   *
   * @return {number|undefined}
  **/
  getFirstFreshIndex() {
    for(var index of this[requestDb$].values()) break;
    return index;
  }

  /**
   * returns log entry index for a given update request id
   *
   * requestId must be fresh enough to be remembered
   *
   * @param {string|Buffer} requestId
   * @return {number|undefined}
  **/
  getRid(requestId) {
    if ('string' === typeof requestId && requestId.length === REQUEST_LOG_ENTRY_BASE64_LENGTH) {
      return this[requestDb$].get(requestId);
    }
    else if (isBuffer(requestId) && requestId.length === REQUEST_LOG_ENTRY_LENGTH) {
      return this[requestDb$].get(requestId.toString('base64'));
    }
    throw new TypeError("FileLog.getRid: requestId must be a 16 characters base64 string or a 12 byte buffer");
  }

  /**
   * appends a checkpoint type entry to the log with the given term
   *
   * @param {number} term
   * @return {Promise}
  **/
  appendCheckpoint(term) {
    if (!isValidTerm(term)) return Promise.reject(new Error("FileLog.appendCheckpoint: term is invalid"));

    return synchronize(this, () => {
      var entry = this._checkpointLogEntry;
      if (entry === undefined) {
        entry = this._checkpointLogEntry = buildLogEntry(null, LOG_ENTRY_TYPE_CHECKPOINT, term, ENTRY_CHECKPOINT_DATA);
      }
      else entry.writeEntryTerm(term);

      debug('appending checkpoint entry with term: %s', term);

      return this._lastIndexFile(indexFile => this._append(indexFile, entry, term));
    });
  }

  /**
   * appends a state type entry to the log with the given term
   *
   * @param {string|Buffer} requestId
   * @param {number} term
   * @param {Buffer} data
   * @return {Promise}
  **/
  appendState(requestId, term, data) {
    return this.appendEntry(requestId, LOG_ENTRY_TYPE_STATE, term, data);
  }

  /**
   * appends a config type entry to the log with the given term
   *
   * @param {string|Buffer} requestId
   * @param {number} term
   * @param {Buffer} data
   * @return {Promise}
  **/
  appendConfig(requestId, term, data) {
    return this.appendEntry(requestId, LOG_ENTRY_TYPE_CONFIG, term, data);
  }

  /**
   * appends an entry to the log with the given term
   *
   * @param {string|Buffer} requestId
   * @param {number} type
   * @param {number} term
   * @param {Buffer} data
   * @return {Promise}
  **/
  appendEntry(requestId, type, term, data) {
    var logEntryTypeBuf = logEntryTypeBuffers[type];
    if (logEntryTypeBuf === undefined) return Promise.reject(new TypeError("FileLog.appendEntry: type is invalid"));
    if (!isValidTerm(term)) return Promise.reject(new Error("FileLog.appendEntry: term is invalid"));

    return synchronize(this, () => {
      var requestKey;
      if ('string' === typeof requestId && requestId.length === REQUEST_LOG_ENTRY_BASE64_LENGTH) {
        requestKey = requestId;
        requestId = Buffer.from(requestId, 'base64');
      } else if (isBuffer(requestId) && requestId.length === REQUEST_LOG_ENTRY_LENGTH) {
        requestKey = requestId.toString('base64');
      } else {
        throw new TypeError("FileLog.appendEntry: requestId must be a 16 characters base64 string or a 12 byte buffer");
      }

      return this._lastIndexFile(indexFile => {
        var termBuf = this._termLogEntryBuf || (this._termLogEntryBuf = Buffer.allocUnsafe(7));
        termBuf.writeUIntLE(term, 0, 7, true);

        debug('appending type: %s entry: (%s) with term: %s', type, data.length, term);

        return this._append(indexFile, [[requestId, logEntryTypeBuf, termBuf, data]], term)
        .then(lastIndex => {
          if (!requestId.equals(zeroRequestBuf) && !hasRequestExpired(requestId)) {
            this[requestDb$].set(requestKey, lastIndex);
          }
          return lastIndex;
        });
      });

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
        return this._createNewIndexFile(indexFile, indexFile => this._append(indexFile, entry, term));
      }
    });
  }

  /**
   * appends log entries to the log optionally truncating it first to the given index
   *
   * entries must consist of buffers representing properly encoded entry data
   *
   * provided entries array may be empty
   *
   * @param {Array} entries
   * @param {number} [index]
   * @return {Promise}
  **/
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

        const write = (indexFile, index, entries) => indexFile.writev(entries, index)
        .then(([numUnwritten, nextEntry]) => {
          var lastWritten = nextEntry - index - 1
            , entry, requestKey, i;
          this[lastIndexFile$] = indexFile;
          if (lastWritten >= 0) {
            for(i = 0; i <= lastWritten; ++i) {
              entry = entries[i];
              if (zeroRequestBuf.compare(entry, REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_END) !== 0
                  && !hasRequestExpired(entry, REQUEST_LOG_ENTRY_OFFSET)) {
                requestKey = entry.toString('base64', REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_END);
                rdb.set(requestKey, index + i);
              }
            }
            this.lastIndex = nextEntry - 1;
            this.lastTerm = entries[lastWritten].readUIntLE(TERM_LOG_ENTRY_OFFSET, 7, true);
          }
          if (numUnwritten !== 0) {
            return this._createNewIndexFile(indexFile, indexFile => write(indexFile, nextEntry, entries.slice(-numUnwritten)));
          }
          else {
            debug('appended log (%s) indexes: %s - %s last term: %s', numEntries, firstIndex, this.lastIndex, this.lastTerm);
          }
        });

        return this._lastIndexFile(indexFile => write(indexFile, index, entries));
      });
    });
  }

  _truncate(index) {
    var nextIndex = this.lastIndex + 1;
    if (index < this.firstIndex || index > nextIndex) return Promise.reject(new Error("FileLog.truncate: index out of index range"));
    if (index === nextIndex) return Promise.resolve(); /* no-op */

    var lastIndex = index - 1;

    const rdb = this[requestDb$], rdbsize = rdb.size;
    for(let [key, idx] of rdb) {
      if (idx >= index) rdb.delete(key);
    }

    debug('truncating log before: %s last: %s rdb: -%s', index, this.lastIndex, rdbsize - rdb.size);

    var truncate = (lastIndexFile) => {
      if (lastIndexFile.allowed(lastIndex)) {
        return lockExclusive(lastIndexFile, () => readTermAt(lastIndexFile, lastIndex).then(term => {
          this.lastIndex = lastIndex;
          this.lastTerm = term;
          return lastIndexFile.truncate(index);
        }));
      }
      else if (index === this.firstIndex && index === lastIndexFile.firstAllowedIndex) {
        return lockExclusive(lastIndexFile, () => {
          this.lastIndex = lastIndex;
          this.lastTerm = this.snapshot.logTerm;
          return lastIndexFile.truncate(index);
        });
      }
      else {
        return this._indexFileOf(lastIndexFile.firstAllowedIndex - 1, 
          prevIndexFile => readTermAt(prevIndexFile, prevIndexFile.lastAllowedIndex)
          .then(term => {
            this.lastIndex = prevIndexFile.lastAllowedIndex;
            this.lastTerm = term;
            this[lastIndexFile$] = prevIndexFile;
            this[indexFileCache$].delete(lastIndexFile.basename);
            debug('deleting log file: %s', lastIndexFile);
            return lockExclusive(lastIndexFile, () => lastIndexFile.destroy().then(() => {
              this._pruneFileNamesCache(lastIndexFile.basename);
              return prevIndexFile;
            }));
          })
        ).then(truncate);
      }
    };

    return this._lastIndexFile().then(truncate);
  }

  /**
   * read log entry at the given index into the buffer
   *
   * resolves to a buffer or a buffer slice containing the whole entry data
   *
   * if buffer is not provided or is too small a new buffer will be created
   *
   * @param {number} index
   * @param {Buffer} [buffer]
   * @return {Promise}
  **/
  getEntry(index, buffer) {
    return this._indexFileOf(index, indexFile => {
      if (isBuffer(buffer) && buffer.length >= indexFile.getByteSize(index, 1)) {
        return indexFile.readb(index, 1, buffer, 0).then(length => buffer.slice(0, length));
      }
      else return indexFile.read(index, 1);
    });
  }

  /**
   * read log entries from the given first index up to the last index
   *
   * resolves to an array of buffers, each buffer representing an entry
   *
   * @param {number} firstIndex
   * @param {number} lastIndex
   * @return {Promise}
  **/
  getEntries(firstIndex, lastIndex) {
    if (lastIndex > this.lastIndex) return Promise.reject(new Error("FileLog.getEntries: lastIndex too large"));
    const result = [];
    if (lastIndex < firstIndex) return Promise.resolve(result);

    const next = (index) => this._indexFileOf(index, indexFile => {
      const maxIndex = min(indexFile.lastAllowedIndex, lastIndex);
      return indexFile.readv(index, maxIndex - index + 1);
    })
    .then(entries => {
      push.apply(result, entries);
      const nextIndex = index + entries.length;
      return (nextIndex <= lastIndex) ? next(nextIndex) : result;
    });

    return next(firstIndex);
  }

  /**
   * read log entries from the given first index up to the last index
   *
   * reads as many entries as fits into the given buffer
   * if the provided buffer is too small a new buffer will be created
   * of the same size as the first entry
   *
   * resolves to an array of buffer views, each buffer representing an entry
   *
   * @param {number} firstIndex
   * @param {number} lastIndex
   * @param {Buffer} buffer
   * @return {Promise}
  **/
  readEntries(firstIndex, lastIndex, buffer) {
    const buflen = buffer.length;
    if (lastIndex > this.lastIndex) return Promise.reject(new Error("FileLog.readEntries: lastIndex too large"));
    const result = [];
    if (lastIndex < firstIndex) return Promise.resolve(result);
    var offset = 0;

    const next = (index) => this._indexFileOf(index, indexFile => {
      const maxIndex = min(indexFile.lastAllowedIndex, lastIndex)
          , count = indexFile.countEntriesFitSize(index, maxIndex - index + 1, buflen - offset);

      if (count !== 0) return indexFile.readb(index, count, buffer, offset)
                        .then(size => {
                          assert(size > 0);
                          const entries = indexFile.splitb(index, count, buffer, offset);
                          push.apply(result, entries);
                          offset += size;
                          return index + count;
                        });
      else return lastIndex + 1;
    })
    .then(nextIndex => {
      if (nextIndex <= lastIndex) {
        return next(nextIndex);
      }
      else if (result.length === 0) {
        /* nothing fit into the buffer */
        debug('won\'t fit index: %s in %s', firstIndex, buflen);
        return this.getEntry(firstIndex).then(entry => [entry]);
      }
      else return result;
    });

    return next(firstIndex);
  }

  /**
   * Create a LogStream instance that streams file log entries
   *
   * @param {number} firstIndex - first index to read
   * @param {number} lastIndex - last index to read
   * @param {Object} [options] - LogStream options
   * @return {LogStream}
  **/
  createEntriesReadStream(firstIndex, lastIndex, options) {
    if (!isValidIndex(firstIndex) || firstIndex < this.firstIndex || firstIndex > this.lastIndex) {
      throw new TypeError("FileLog.streamEntries: firstIndex must be a valid index");
    }
    if (!isValidIndex(lastIndex) || lastIndex < this.firstIndex || lastIndex > this.lastIndex) {
      throw new TypeError("FileLog.streamEntries: lastIndex must be a valid index");
    }
    return new LogStream(this, firstIndex, lastIndex, options);
  }

  /**
   * read a term at the given index
   *
   * resolves to {number}
   *
   * @param {number} index
   * @return {Promise}
  **/
  termAt(index) {
    /* hot paths */
    if (index === this.lastIndex) return Promise.resolve(this.lastTerm);
    else if (index === this.snapshot.logIndex) return Promise.resolve(this.snapshot.logTerm);
    else if (index < this.firstIndex || index > this.lastIndex) return Promise.resolve();
    /* slow path */
    return this._indexFileOf(index, indexFile => readTermAt(indexFile, index));
  }

  /**
   * creates a temporary snapshot file in the same directory as the current snapshot
   *
   * when the snapshot is complete one can install it with installSnapshot method
   *
   * @param {number} index - snapshot index
   * @param {number} term - snapshot term
   * @param {number|stream.Reader} dataSize - snapshot data size or a stream.Reader instance
   * @return {Promise}
  **/
  createTmpSnapshot(index, term, dataSize) {
    return new SnapshotFile(createTempName(this.snapshot.filename), index, term, dataSize);
  }

  /**
   * install snapshot file instance replacing current snapshot
   *
   * @param {SnapshotFile} snapshot - snapshot file instance to install
   * @param {boolean} [compactOnly] - allow only compacting snapshot
   * @return {Promise}
  **/
  installSnapshot(snapshot, compactOnly) {
    if (snapshot instanceof SnapshotFile) {
      if (this.readOnly) Promise.reject(new Error("FileLog is in read-only mode"));
      return snapshot.ready().then(() => synchronize(this, () => lockExclusive(this.snapshot, () => {
        var currentSnapshot = this.snapshot;
        if (snapshot === currentSnapshot) return;
        return this.termAt(snapshot.logIndex).then(term => {
          if (snapshot.logTerm === term) {
            /* compaction snapshot */
            debug('installing compaction snapshot index: %s term: %s dataSize: %s', snapshot.logIndex, snapshot.logTerm, snapshot.dataSize);
            debug('replacing snapshot index: %s term: %s dataSize: %s', currentSnapshot.logIndex, currentSnapshot.logTerm, currentSnapshot.dataSize);
            this.snapshot = snapshot;
            this.firstIndex = snapshot.logIndex + 1;
            /* TODO: wipe out obsolete log files in the background, this requires synchronization with
               any current pending reads that began before this.firstIndex modification */
           return currentSnapshot.close().then(() => snapshot.replace(currentSnapshot.filename));
          }
          else if (!compactOnly) {
            /* discard the entire log (rename logdir, create new log dir, new index file, new caches etc) */
            /* during this operation any attempt to read log files will end up with error */
            return currentSnapshot.close().then(() => snapshot.replace(currentSnapshot.filename))
            .then(() => createNewLogDirectory.call(this, snapshot));
           }
          else throw new TypeError("FileLog.installSnapshot: the snapshot is not a compaction of the log");
        });
      })));
    }
    else throw new TypeError("FileLog.installSnapshot: the snapshot must be an instance of the SnapshotFile");
  }

  /**
   * watch install snapshot directory for compacting snapshot and install it automatically
   *
   * @param {string} filename - install snapshot filename
   * @return {Promise}
  **/
  watchInstallSnapshot(filename) {
    const dirname = path.dirname(filename)
        , basename = path.basename(filename);

    debug('watching install snapshot file: %s', filename);

    return synchronize(this, () => mkdirp(dirname)
    .then(created => {
      var watcher = this.installSnapshotWatcher;
      if (created) debug('created install snapshot directory: %s', dirname);
      if (watcher) {
        watcher.close();
        debug('install snapshot watcher closed');
        this.installSnapshotWatcher = null;
      }

      const checkWatcher = () => (watcher && this.installSnapshotWatcher === watcher);

      const installer = () => {
        if (checkWatcher()) {
          new SnapshotFile(filename).ready()
          .then(snapshot => {
            if (checkWatcher()) {
              return this.installSnapshot(snapshot, true);
            }
            else return snapshot.close();
          })
          .then(() => checkWatcher() && setTimeout(startWatching, INSTALL_SNAPSHOT_WATCHER_COOLDOWN_INTERVAL).unref())
          .catch(err => {
            console.error('FileLog: install snapshot failed to open: %s', err);
            checkWatcher() && setTimeout(startWatching, INSTALL_SNAPSHOT_WATCHER_COOLDOWN_INTERVAL).unref();
          });
        }
      };

      const handler = (type, name) => {
        if (type === 'rename' && name === basename) {
          watcher.removeListener('change', handler);
          installer();
        }
      };

      const startWatching = () => {
        if (checkWatcher()) {
          access(filename, R_OK | W_OK).then(installer, err => {
            if (checkWatcher()) {
              watcher.on('change', handler);
              debug('install snapshot watching for changes in: %s', filename);
            }
          });
        }
      };

      this.installSnapshotWatcher = watcher = watch(dirname)
      .on('error', err => {
        console.error('FileLog: install snapshot watcher error: %s', err);
        if (watcher) {
          if (this.installSnapshotWatcher === watcher) this.installSnapshotWatcher = null;
          watcher.close();
          watcher = null;
          debug('install snapshot watcher closed');
        }
      });

      startWatching();
    }));
  }

  /**
   * feed stateMachine with content of this log
   *
   * resolves to stateMachine.lastApplied
   *
   * @param {StateMachineBase} state
   * @param {number} [lastIndex]
   * @param {number} [currentTerm]
   * @return {Promise}
  **/
  feedStateMachine(stateMachine, lastIndex, currentTerm) {
    var snapshot
      , firstIndex = this.firstIndex
      , lastApplied = stateMachine.lastApplied;
    if (lastIndex === undefined) lastIndex = this.lastIndex;
    if (currentTerm === undefined) currentTerm = this.lastTerm;
    if (lastIndex > this.lastIndex || lastIndex < firstIndex - 1) return Promise.reject(new Error("last index not in the file log range"));
    if (lastIndex <= lastApplied) return Promise.resolve(lastApplied);

    if (lastApplied < this.snapshot.logIndex) {
      snapshot = this.snapshot;
    }
    else firstIndex = lastApplied + 1;

    if (lastIndex - firstIndex < FEED_STATE_NUM_ENTRIES_TRESHOLD) {
      return this.getEntries(firstIndex, lastIndex)
      .then(entries => stateMachine.applyEntries(entries, firstIndex, currentTerm, snapshot));
    }
    else return new Promise((resolve, reject) => {
      this.createEntriesReadStream(firstIndex, lastIndex)
      .on('error', reject)
      .pipe(new StateMachineWriter(stateMachine, firstIndex, currentTerm, snapshot))
      .on('error', reject)
      .on('finish', () => resolve(stateMachine.lastApplied));
    });
  }

  // firstIndexOfTerm(term) {

  // }

  /* PRIVATE API */

  _createNewIndexFile(lastIndexFile, callback) {
    if (this.readOnly) Promise.reject(new Error("FileLog is in read-only mode"));
    var index = lastIndexFile.lastAllowedIndex + 1;
    var basename = logBaseName(index);
    lastIndexFile = this[indexFileCache$].get(basename);
    if (!lastIndexFile) {
      debug('creating new index file: %s', index);
      lastIndexFile = new IndexFile(this.logdir, index);
      this[indexFileCache$].add(lastIndexFile);
      this._pruneFileNamesCache(basename);
    }
    return lockShared(lastIndexFile, () => lastIndexFile.ready().then(callback));
  }

  _lastIndexFile(callback) {
    if (this.readOnly) Promise.reject(new Error("FileLog is in read-only mode"));
    const lastIndexFile = this[lastIndexFile$];
    if (callback) return lockShared(lastIndexFile, () => lastIndexFile.ready().then(callback));
    return lastIndexFile.ready();
  }

  _indexFileOf(index, callback) {
    const lastIndexFile = this[lastIndexFile$];
    if (lastIndexFile && lastIndexFile.isReady && lastIndexFile.includes(index)) {
      /* hot path */
      return lockShared(lastIndexFile, () => {
        if (callback) return lastIndexFile.ready().then(callback);
        else return lastIndexFile.ready();
      });
    }
    const found = (basename) => {
      if (basename !== undefined && index <= this.lastIndex) {
        var indexFile = this[indexFileCache$].get(basename);
        if (indexFile === undefined) {
          debug('opening index file: %s', basename);
          indexFile = new IndexFile(logPath(this.logdir, basename));
          this[indexFileCache$].add(indexFile);
        }
        return lockShared(indexFile, ()=> indexFile.ready().then(indexFile => {
          if (!indexFile.includes(index)) {
            throw new Error("FileLog: could not find index file for: " + index.toString(16));
          }
          if (callback) return callback(indexFile);
          return indexFile;
        }));
      }
      throw new Error("FileLog: could not find index file for: " + index.toString(16));
    };

    return this._indexFileNameOf(index).then(found);
  }

  /* refresh indexFileNames when files destroyed/new created */
  _pruneFileNamesCache(index) {
    var basename = logBaseName(index);
    var prefix = basename.substr(0, INDEX_PATH_PREFIX_LENGTH);
    this[indexFileNames$].delete(prefix);
  }

  _indexFileNameOf(index) {
    if (index < this.firstIndex || index > this.lastIndex) return Promise.resolve();
    var basename = logBaseName(index);
    var prefix = basename.substr(0, INDEX_PATH_PREFIX_LENGTH);
    var indexFileNames = this[indexFileNames$];
    var proment = indexFileNames.get(prefix);
    if (proment === undefined) {
      debug('no cached names for prefix: %s, reading directory', prefix);
      proment = readdir(path.dirname(logPath(this.logdir, basename)))
      .then(entries => entries.filter(file => file.length === INDEX_FILENAME_LENGTH && file.endsWith(INDEX_FILE_EXT))
                     .map(file => file.substr(0, INDEX_BASENAME_LENGTH))
                     .sort());
      indexFileNames.set(prefix, proment);
    }
    return proment.then(entries => bSearch(entries, basename));
  }

  _findRealLastIndexFile(firstIndexFile, lastIndexFile, readOnly) {
    if (firstIndexFile === lastIndexFile || lastIndexFile.nextIndex > lastIndexFile.firstAllowedIndex) {
      this[indexFileNames$].clear();
      return Promise.resolve(lastIndexFile);
    }
    else {
      debug('last index file: %s is empty, scanning for the last entry', lastIndexFile);
      this[indexFileCache$].delete(lastIndexFile.basename);
      this.lastIndex = lastIndexFile.nextIndex - 1;
      if (!readOnly) this[lastIndexFile$] = lastIndexFile;
      return this._indexFileOf(this.lastIndex)
        .then(indexFile => lastIndexFile[readOnly ? 'close' : 'destroy']()
                        .then(() => this._findRealLastIndexFile(firstIndexFile, indexFile, readOnly)));
    }
  }

  _readRequestsFromLogs(logFirstAllowedIndex) {
    debug("reading request ids from index files down to: %s", logFirstAllowedIndex);
    var lastIndexFile = this[lastIndexFile$];
    var rdb = this[requestDb$];
    rdb.clear();
    var temprdb = [];
    var numExpired = 0;
    var requestId = Buffer.allocUnsafe(REQUEST_LOG_ENTRY_LENGTH);
    var start = Date.now();
    var read = (indexFile, index) => {
      if (indexFile.includes(index)) {
        return indexFile.readSlice(index, REQUEST_LOG_ENTRY_OFFSET, REQUEST_LOG_ENTRY_END, requestId).then(() => {
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
      else if (index >= logFirstAllowedIndex && index < indexFile.firstAllowedIndex) {
        return this._indexFileOf(index).then(indexFile => read(indexFile, index));
      }
      else return Promise.resolve();
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
        const rdb = this[requestDb$]
            , rdbsize = rdb.size;
        for(var req of rdb.keys()) {
          if (hasRequestExpired(req)) {
            rdb.delete(req);
          }
          else break;
        }
        if (rdb.size !== rdbsize) {
          debug("rdb cleanup: forgot about %s requests", rdbsize - rdb.size);
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

/* slow read */
function readTermAt(indexFile, index) {
  var buffer = Buffer.allocUnsafe(7);
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
  const oldIndexFileCache = this[indexFileCache$];
  const indexFileCache = this[indexFileCache$] = new IndexFileCache(this);
  this[lastIndexFile$] = null;
  this[requestDb$].clear();

  this.firstIndex = snapshot.logIndex + 1;
  this.lastIndex = snapshot.logIndex;
  this.lastTerm = snapshot.logTerm;
  this.snapshot = snapshot;

  return oldIndexFileCache.close().then(() => {
    const logdir = this.logdir;
    return renameSyncDir(logdir, createRotateName(logdir))
    .then(() => mkdirp(logdir))
    .then(created => {
      if (!created) throw new Error("FileLog: can't create new file log - logdir still exists");
      const indexFile = new IndexFile(logdir, snapshot.logIndex + 1);
      indexFileCache.add(indexFile);
      this[indexFileNames$].clear();
      this[lastIndexFile$] = indexFile;
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


function initializeWritableLogFile(logdir, snapshotfile) {
  debug('opening log: "%s"', logdir);

  defineConst(this, 'readOnly', false);

  return mkdirp(logdir).then(() => Promise.all([
    findFirstOrLastIndexFile(logdir, 0),
    findFirstOrLastIndexFile(logdir, -1),
    new SnapshotFile(snapshotfile).ready().catch(err => {
      if (err.code !== 'ENOENT') throw err;
      /* snapshot file not found, so create a new one */
      return new SnapshotFile(snapshotfile, 0, 0, 0).ready();
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
      /* set the last index file hot path */
      this[lastIndexFile$] = lastIndexFile;
      /* is entire log empty? */
      if (this.lastIndex === this.snapshot.logIndex) {
        this.lastTerm = this.snapshot.logTerm;
      }
      /* nope, then read last term from the log */
      else return readTermAt(lastIndexFile, this.lastIndex)
                  .then(term => (this.lastTerm = term));
    })
    .then(() => this._readRequestsFromLogs(firstIndexFile.firstAllowedIndex))
  })
  .then(() => {
    cleanupTempFiles(this.snapshot.filename, debug).catch(err => debug('temporary files cleanup ERROR: %s', err));
  });
}

function initializeReadOnlyLogFile(logdir, snapshotfile) {
  debug('opening read-only log: "%s"', logdir);

  defineConst(this, 'readOnly', true);

  return Promise.all([
    findFirstOrLastIndexFile(logdir, 0),
    findFirstOrLastIndexFile(logdir, -1),
    new SnapshotFile(snapshotfile).ready()
  ])
  .then(([firstIndexPath, lastIndexPath, snapshot]) => {
    this.snapshot = snapshot;

    var firstIndexFile, lastIndexFile;
    if (firstIndexPath === undefined || lastIndexPath === undefined) {
      /* create new index file */
      throw new Error("no log files found");
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
    return this._findRealLastIndexFile(firstIndexFile, lastIndexFile, true)
    .then(lastIndexFile => {
      this.lastIndex = lastIndexFile.nextIndex - 1;
      if (this.snapshot.logIndex >= this.lastIndex && this.lastIndex >= this.firstIndex) {
        throw new Error("snapshot last index covers entire log: " + this.snapshot.logIndex);
      }
      /* is entire log empty? */
      if (this.lastIndex === this.snapshot.logIndex) {
        this.lastTerm = this.snapshot.logTerm;
        return lastIndexFile;
      }
      /* nope, then read last term from the log */
      else return readTermAt(lastIndexFile, this.lastIndex)
                  .then(term => {
                    this.lastTerm = term;
                    return lastIndexFile
                  });
    })
    .then(lastIndexFile => {
      /* close the last index file */
      this[lastIndexFile$] = null;
      if (firstIndexFile !== lastIndexFile) {
        this[indexFileCache$].delete(lastIndexFile.basename);
        return lastIndexFile.close();
      }
    });
  });
}

function initializeLogFile(logdir, snapshotfile, readOnly) {
  return readOnly ? initializeReadOnlyLogFile.call(this, logdir, snapshotfile)
                  : initializeWritableLogFile.call(this, logdir, snapshotfile);
}

class IndexFileCache extends Map {
  constructor(fileLog) {
    if (!(fileLog instanceof FileLog)) throw TypeError("IndexFileCache requires instance of FileLog");
    super();
    this.fileLog = fileLog;
  }

  close() {
    const files = Array.from(this.values());
    this.clear();
    return Promise.all(files.map(indexFile => lockExclusive(indexFile, () => indexFile.close())));
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
    const basename = indexFile.basename;
    /* sanity check */
    if (this.has(basename)) throw new Error("IndexFileCache.add: there is already a cached index file at " + basename);
    this.set(basename, indexFile);
    if (this.size >= CACHE_INDEX_FILES_LIMIT_CAPACITY_HI) this.purge();
  }

  purge() {
    debug('purging index file cache');
    var lastIndexFile = this.fileLog[lastIndexFile$];
    var size = this.size;
    for(let indexFile of this.values()) {
      if (size-- <= CACHE_INDEX_FILES_LIMIT_CAPACITY_LO) break;
      /* sanity: don't purge last index file or fresh files */
      if (indexFile !== lastIndexFile && indexFile.isReady) {
        debug('will purge: %s', indexFile);
        this.delete(indexFile.basename);
        lockExclusive(indexFile, () => indexFile.close());
      }
    }
  }
}

/*
var ben=require('ben')
var genIdent = require('./raft/id').genIdent;
var FileLog = require('./raft/filelog');

var log = new FileLog('tmp/backup/01/log','tmp/backup/01/snap');log.ready().then(r=>console.log('ret: %s', r), console.error)
ben.async(1000, cb=>log.termAt(37009).then(cb,console.warn),ms=>console.log('ms: %s',ms))

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