#!/usr/bin/env node
/* 
 *  Copyright (c) 2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

if (require.main !== module) throw new Error("zr-tool.js must be run directly from node");

const path = require('path')
    , fs = require('fs')
    , assert = require('assert')
    , { EOL } = require('os')
    , { format } = require('util')
    , { Writable, Transform, PassThrough } = require('stream');

const { createUnzip, Z_BEST_COMPRESSION } = require('zlib');

const mp = require('msgpack-lite');

const program = require('commander')
    , debug = require('debug')('zmq-raft:tool');

const pkg = require('../package.json');

const raft = require('..');

const { 
      FTYPE_DIRECTORY
    , FTYPE_FILE
    , FTYPE_INDEXFILE
    , FTYPE_SNAPSHOTFILE
    , detect
    } = require('../lib/utils/filetype');

const { server: { FileLog }
      , common: { SnapshotFile, IndexFile, FilePersistence
                , LogEntry: { readers: { readTypeOf, readRequestIdOf, readTermOf, readDataOf } }
                }
      , utils: { fsutil, helpers: { lpad } }
      } = raft;

const numpadder = "0".repeat(256)
    , numpad = (num, base, size) => lpad(num.toString(base), size, numpadder);
const hexsize = (num) => (Math.log2(num)/4 + 1|0);

var outstrm = process.stdout;
const output = (...args) => outstrm.write(format(...args) + EOL);

program
  .version(pkg.version)
  .description('inspect and modify various zmq-raft file formats')
  .option('-o, --output <file>', 'file for output, by default output goes to stdout')
  .option('-s, --snapshot <path>', 'alternative path to snapshot file (FileLog)', 'snap')
  .option('-l, --log <path>', 'alternative path to log sub-directory (FileLog)', 'log')
  // .option('-y, --yes', 'confirm dangerous operation')
  // .option('-j, --data <json>', 'appended data')
  // .option('-f, --from <file>', 'appended data filename')
  // .option('-Z, --no-zip', 'do not zip when creating snapshot')
  // .option('-t, --term <term>', 'raft term')

program.command('dump <file>').alias('d')
  .description('dump the content of a snapshot, state or log index file')
  .option('-i, --index <ranges>', 'log index or index ranges to dump')
  .option('-d, --dest <dir>', 'target directory for dumping log entries, default is CWD', '.')
  .option('-U, --no-unzip', 'do not unzip when dumping snapshot')
  .option('-m, --msgpack', 'interpret snapshot/log data as msgpack and dump as JSON')
  .action((file, options) => run('dump', file, options));

program.command('list <path>').alias('l')
  .description('list the content of the log directory or state or index file')
  .option('-i, --index <ranges>', 'log index or index ranges to list')
  .action((path, options) => run('list', path, options));

program.command('inspect <path>').alias('*')
  .description('inspect log directory, indexfile or state file')
  .action(path => run('inspect', path));

program.parse(process.argv);

if (program.args.length !== 2) program.help();

function run(command, path, options) {
  return Promise.resolve().then(() => {
    if (program.output) {
      return new Promise((resolve, reject) => {
        outstrm = fs.createWriteStream(program.output)
        .on('error', reject)
        .on('open', resolve);
      });
    }
  })
  .then(() => {
    switch(command) {
      case 'inspect': return inspect(path);
      case 'list': return list(path, options);
      case 'dump': return dump(path, options);
      default:
        throw new Error("unimplemented, sorry!");
    }
  })
  .then(() => {
    if (outstrm !== process.stdout) return new Promise((resolve, reject) => {
      outstrm.on('error', reject).on('close', resolve).end();
    });
  })
  .catch(err => {
    console.error('%s: %s', err.name, err.message);
    // console.error(err.stack);
    process.exit(1);
  });
}

function dump(file, options) {
  return detect(file).then(([type, stat, arg]) => {
    switch(type) {
      case FTYPE_INDEXFILE:
        return dumpIndexFile(file, stat, arg, options);
      case FTYPE_SNAPSHOTFILE:
        return dumpSnapshotFile(file, stat, arg, options);
      case FTYPE_FILE:
        return dumpState(file, stat, arg);
      case FTYPE_DIRECTORY:
        throw new Error("you may only dump files");
    }
  });
}

function list(file, options) {
  return detect(file).then(([type, stat, arg]) => {
    switch(type) {
      case FTYPE_DIRECTORY:
        return listFileLog(file, stat);
      case FTYPE_INDEXFILE:
        return listIndexFile(file, stat, arg, options);
      case FTYPE_SNAPSHOTFILE:
      case FTYPE_FILE:
        throw new Error("you may only list dir:log or file:index");
    }
  });
}

function inspect(file) {
  return detect(file).then(([type, stat, arg]) => {
    switch(type) {
      case FTYPE_DIRECTORY:
        return inspectFileLog(file, stat);
      case FTYPE_FILE:
        return inspectState(file, stat, arg);
      case FTYPE_INDEXFILE:
        return inspectIndexFile(file, stat, arg);
      case FTYPE_SNAPSHOTFILE:
        return inspectSnapshotFile(file, stat, arg);
    }
  });
}

function openFileLog(dir) {
  return new FileLog(path.join(dir, program.log), path.join(dir, program.snapshot), true).ready();
}

function inspectFileLog(dir, stat) {
  return openFileLog(dir)
  .then(filelog => {
    var tokenfile = filelog.snapshot.makeTokenFile();
    return fsutil.fstat(tokenfile.fd).then(stat => reportSnapshot(filelog.snapshot, stat, tokenfile))
    .then(snapshotReport => {
      displayReport({
        type: "dir:log",
        path: dir,
        firstIndex: filelog.firstIndex,
        lastIndex: filelog.lastIndex,
        lastTerm: filelog.lastTerm,
        snapshot: snapshotReport
      });

      return filelog.close();
    });
  });
}

function listFileLog(dir, stat) {
  return openFileLog(dir)
  .then(filelog => FileLog.readIndexFileNames(filelog.logdir)
    .then(files => {
      files.forEach(file => output(file));
      return filelog.close();
    })
  );
}

function inspectSnapshotFile(file, stat, tokenfile) {
  return new SnapshotFile(file).ready()
  .then(snapshot => reportSnapshot(snapshot, stat, tokenfile)
    .then(report => {
      displayReport(report);
      return Promise.all([tokenfile.close(), snapshot.close()]);
    })
  );
}

function reportSnapshot(snapshot, stat, tokenfile) {
  return tokenfile.readTokenData('META', 0, snapshot.dataOffset)
  .then(meta => reportMeta(meta, {
      type: "file:snapshot",
      path: snapshot.filename,
      fileSize: stat.size,
      logIndex: snapshot.logIndex,
      logTerm: snapshot.logTerm,
      dataSize: snapshot.dataSize
    })
  );
}

function dumpSnapshotFile(file, stat, tokenfile, options) {
  return new SnapshotFile(file).ready()
  .then(snapshot => new Promise((resolve, reject) => {
      var reader = snapshot.createDataReadStream().on('error', reject);
      if (options.unzip) reader = reader.pipe(createUnzip()).on('error', reject);
      if (options.msgpack) reader = reader.pipe(mp.createDecodeStream())
                                      .on('error', reject)
                                      .pipe(createJsonTransform())
                                      .on('error', reject);
      reader.on('end', resolve)
      .pipe(outstrm, { end: false })
      .on('error', reject);
    })
    .then(() => Promise.all([tokenfile.close(), snapshot.close()]))
  );
}

function inspectIndexFile(file, stat, tokenfile) {
  return new IndexFile(file).ready()
  .then(indexfile => tokenfile.readTokenData('META', 0, indexfile.position)
    .then(meta => {
      var entryCount = indexfile.capacity - indexfile.free
        , report = reportMeta(meta, {
        type: "file:index",
        path: file,
        fileSize: stat.size,
        dataSize: indexfile.getByteSize(indexfile.firstAllowedIndex, entryCount),
        indexSize: indexfile.offsets.buffer.byteLength,
        capacity: indexfile.capacity,
        basename: indexfile.basename,
        firstAllowedIndex: indexfile.firstAllowedIndex,
        lastAllowedIndex: indexfile.lastAllowedIndex,
        nextIndex: indexfile.nextIndex,
        entryCount: entryCount,
        freeCount: indexfile.free
      });

      displayReport(report);

      return Promise.all([tokenfile.close(), indexfile.close()]);
    })
  );
}

function listIndexFile(file, stat, tokenfile, options) {
  return new IndexFile(file).ready()
  .then(indexfile => {
    var padindex = hexsize(indexfile.lastAllowedIndex)
      , padsize = hexsize(stat.size)
      , indexes = parseRanges(options.index, indexfile.firstAllowedIndex, indexfile.nextIndex - 1);

    indexes.forEach(([first, last]) => {
      for(var index = first; index <= last; ++index) {
        var size = indexfile.getByteSize(index, 1)
          , offset = indexfile.getByteOffset(index);
        output("%s:%s:%s",
          numpad(index, 16, padindex),
          numpad(offset, 16, padsize),
          numpad(size, 16, padsize));
      }
    });

    return Promise.all([tokenfile.close(), indexfile.close()]);
  });
}

// ${hexindex}.meta: type:requestBase64:term e.g.: 0:AAAAAAAAAAAAAAAA:42
// ${hexindex}.data
function dumpIndexFile(file, stat, tokenfile, options) {
  var destdir = options.dest;
  return ((destdir === '.' || destdir === '..') ? Promise.resolve()
                                                : fsutil.mkdirp(destdir))
  .then(() => new IndexFile(file).ready())
  .then(indexfile => {
    var nextIndex = indexfile.nextIndex
      , indexes = parseRanges(options.index, indexfile.firstAllowedIndex, nextIndex - 1);

    const dumpentry = (index, entry) => {
      var basename = path.join(destdir, numpad(index, 16, 16))
        , data = readDataOf(entry);
      output(basename);
      if (options.msgpack) data = JSON.stringify(mp.decode(data));
      return Promise.all([
        fsutil.writeFile(basename + '.meta', format("%s:%s:%s",
          readTypeOf(entry),
          readRequestIdOf(entry, 'base64'),
          readTermOf(entry)
        )),
        fsutil.writeFile(basename + '.data', data, 'utf8')
      ])
    };

    const dumplog = ([firstIndex, lastIndex]) => new Promise((resolve, reject) => {
      var index = firstIndex;
      indexfile.createLogEntryReadStream(firstIndex, lastIndex)
      .on('error', reject)
      .pipe(new Writable({
        objectMode: true,
        highWaterMark: 1,
        write(entry, _, callback) {
          Promise.resolve().then(() => dumpentry(index++, entry))
                           .then(() => callback(null), callback);
        }
      }))
      .on('error', reject)
      .on('finish', resolve);
    });

    return forEachAsync(indexes, dumplog)
    .then(() => Promise.all([tokenfile.close(), indexfile.close()]));
  });
}

function inspectState(file, stat, fd) {
  return new FilePersistence(file, {}).ready()
  .then(persistence => {
    displayReport({
      type: "file:state",
      path: file,
      fileSize: stat.size,
      data: Object.assign({}, persistence[Symbol.for('data')])
    });
    return persistence.close();
  });
}

function dumpState(file, stat, fd) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(null, {fd: fd, autoClose: true})
    .on('error', reject)
    .pipe(mp.createDecodeStream())
    .on('error', reject)
    .on('data', data => output("%j", data))
    .on('end', resolve);
  });
}

function reportMeta(meta, report) {
  if (meta) {
    try {
      report.meta = mp.decode(meta);
    } catch(err) {
      report.error = "error decoding metadata";
    }
  }
  return report;
}

function parseRanges(index, first, last) {
  if (index === undefined) return [[first,last]];
  return index.split(',').map(i => parseRange(i, first, last));
}

function parseRange(index, first, last) {
  if (index === ':') return [first, last];
  var idx, range = index.split(':');
  if (range.length === 1) {
    let idx = parseIndex(range[0], first, last)
    return [idx, idx];
  }
  else if (range.length === 2) {
    if (range[0] === '') return [first, parseIndex(range[1], first, last)];
    else if (range[1] === '') return [parseIndex(range[0], first, last), last];
    else return range.map(idx => parseIndex(idx, first, last));
  }
  else throw new TypeError("expected only beg:end index range");
}

function parseIndex(index, first, last) {
  index = parseInt(index);
  if (!Number.isFinite(index))
    throw new TypeError("index should be a decimal number");
  if (index < first || index > last)
    throw new TypeError(`index does not fit within range ${first}:${last}`);
  return index;
}

function forEachAsync(array, callback) {
  var index = 0;

  const iterate = () => {
    if (index < array.length) {
      return Promise.resolve(array[index++]).then(callback).then(iterate);
    }
  };

  return Promise.resolve(iterate());
}

function createJsonTransform() {
  return new Transform({
    writableObjectMode: true,
    transform(object, _, callback) {
      try {
        var data = JSON.stringify(object) + EOL;
      } catch(err) { return callback(err); }
      callback(null, data);
    }
  });
}

function displayReport(data) {
  output(JSON.stringify(data, null, 2));
}
