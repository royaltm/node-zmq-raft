#!/usr/bin/env node
/* 
 *  Copyright (c) 2016-2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

if (require.main !== module) throw new Error("zr-log-compact.js must be run directly from node");

const path = require('path')
    , fs = require('fs')
    , assert = require('assert');

const { Z_NO_COMPRESSION, Z_BEST_COMPRESSION } = require('zlib');

const program = require('commander')
    , Hjson = require('hjson')
    , debug = require('debug')('zmq-raft:log-compact');

const pkg = require('../package.json');

const raft = require('..');

const { server: { FileLog, builder: { createOptions } }
      , common: { SnapshotFile }
      , client: { ZmqRaftClient }
      , utils: { fsutil: { mkdirp }
               , tempfiles: { createTempName, cleanupTempFiles }
               , helpers: { parsePeers } }
      } = raft;

program
  .version(pkg.version)
  .usage('[options]')
  .option('-c, --config <file>', 'Config file in hjson format')
  .option('-t, --target <file>', 'Target snapshot filename')
  .option('-m, --state-machine <file>', 'State machine')
  .option('-p, --peer <url>', 'Peer url or id to determine last index from commit index')
  .option('-i, --index <n>', 'Last index', parseInt)
  .option('-d, --dir <dir>', 'LogFile root path')
  .option('-l, --log <dir>', 'LogFile alternative directory')
  .option('-s, --snapshot <file>', 'LogFile alternative snapshot path')
  .option('-z, --zip <level>', 'State machine compressionLevel option', parseInt)
  .option('-U, --no-unzip', 'State machine unzipSnapshot option')
  .parse(process.argv);

function mergeSmOptions(options) {
  var smOptions = Object.assign({compressionLevel: Z_BEST_COMPRESSION}, options);

  /* force zip */
  if (program.zip !== undefined) {
    smOptions.compressionLevel = program.zip;
  }

  if ('number' !== typeof smOptions.compressionLevel
      || isNaN(smOptions.compressionLevel)
      || smOptions.compressionLevel < 0
      || smOptions.compressionLevel > Z_BEST_COMPRESSION) {
    throw new TypeError('zip level must be an integer from 0 to ' + Z_BEST_COMPRESSION);
  }

  if (program.unzip === false) {
    /* force no-unzip */
    smOptions.unzipSnapshot = false;
  }
  else if (smOptions.unzipSnapshot === undefined
          && options.compressionLevel !== Z_NO_COMPRESSION) {
    /* set unzip if not set on original options
       and original zip level wasn't no-compression */
    smOptions.unzipSnapshot = true;
  }

  return smOptions;
}

function findPeerUrl(options) {
  var peers = parsePeers(options.peers);
  if ('string' === typeof options.id) {
    return peers.get(options.id);
  }
}

function exitError(status) {
  args = [].slice.call(arguments, 1);
  console.error.apply(console, args);
  process.exit(status);
}

function start() {

  const options = readConfig(program.config);

  const rootDir = program.dir || options.data.path;

  if ('string' !== typeof rootDir) {
    throw new TypeError('data directory must be defined!');
  }

  debug('data directory: %s', rootDir);

  const configDir = program.config ? path.dirname(path.resolve(program.config))
                                   : rootDir;

  const logDir = path.resolve(rootDir, program.log || options.data.log || 'log');
  const snapFile = path.resolve(rootDir, program.snapshot || options.data.snapshot || 'snap');

  const targetFile = program.target ||
                     (options.data.compact.install && path.resolve(rootDir, options.data.compact.install));
  const stateMachinePath = program.stateMachine ||
                           (options.data.compact.state.path && path.resolve(configDir, options.data.compact.state.path));

  if (!targetFile) {
    exitError(2, "no target file");
  }

  debug('target snapshot: %s', targetFile);

  if (!stateMachinePath) {
    exitError(3, "no state machine specified");
  }

  var lastIndexPromise;
  if (isFinite(program.index) && program.index > 0) {
    lastIndexPromise = Promise.resolve(program.index);
  }
  else {
    const peerUrl = program.peer || findPeerUrl(options);
    if (!peerUrl) {
      exitError(4, "specify last index or peer url");
    }
    const client = new ZmqRaftClient({peers: [peerUrl]});
    lastIndexPromise = client.requestLogInfo(true, 5000).then(({commitIndex, pruneIndex}) => {
      client.close();
      return Math.min(commitIndex, pruneIndex);
    });
  }

  const StateMachineClass = loadState(stateMachinePath);

  const stateMachine = new StateMachineClass(mergeSmOptions(options.data.compact.state.options));
  const fileLog = new FileLog(logDir, snapFile, true);

  return Promise.all([
    fileLog.ready(), lastIndexPromise, mkdirp(path.dirname(targetFile))
  ])
  .then(([fileLog, lastIndex]) => Promise.all([
    fileLog.ready(), stateMachine.ready(), lastIndex, fileLog.termAt(lastIndex)
  ]))
  .then(([fileLog, stateMachine, lastIndex, lastTerm]) => {
    var tempSnapshot;

    if (lastTerm === undefined) throw new Error("last index not found in the file log");

    if (stateMachine.snapshotReadStream) {
      tempSnapshot = new SnapshotFile(createTempName(targetFile), lastIndex, lastTerm, stateMachine.snapshotReadStream);
    }

    debug('feeding state machine with log data indexes: %s - %s', stateMachine.lastApplied + 1, lastIndex);

    return fileLog.feedStateMachine(stateMachine, lastIndex)
    .then(lastApplied => {
      assert.strictEqual(lastIndex, lastApplied);

      var tempSnapshotPromise;

      if (!tempSnapshot) {
        if ('function' === typeof stateMachine.createSnapshotReadStream) {
          tempSnapshot = new SnapshotFile(createTempName(targetFile), lastIndex, lastTerm, stateMachine.createSnapshotReadStream());
          tempSnapshotPromise = tempSnapshot.ready();
        }
        else if ('function' === typeof stateMachine.serialize) {
          const snapshotData = stateMachine.serialize();
          tempSnapshot = new SnapshotFile(createTempName(targetFile), lastIndex, lastTerm, snapshotData.length);
          tempSnapshotPromise = tempSnapshot.ready().then(s => s.write(snapshotData, 0, snapshotData.length));
        }
        else {
          throw new Error("Could not determine how to create a snapshot data from the provided state machine.");
        }
      }
      else {
        tempSnapshotPromise = tempSnapshot.ready();
      }
      return Promise.all([tempSnapshotPromise, stateMachine.close()]);
    })
    .then(() => tempSnapshot.replace(targetFile))
    .then(() => cleanupTempFiles(targetFile, debug))
    .then(() => tempSnapshot.close());
  })
  .then(() => debug('compaction done'));
}

Promise.resolve().then(start).catch(err => {
  console.error("LOG COMPACTION ERROR");
  console.error(err.stack);
  process.exit(1);
});

function loadState(filename) {
  filename = path.resolve(filename);
  debug('StateMachineClass: %j', filename);
  return require(filename);
}

function readConfig(configFile) {
  var config;
  if (configFile) {
    debug('reading config: %s', configFile);
    const text = fs.readFileSync(configFile, 'utf8');
    config = Hjson.parse(text);
  }
  else config = {};

  return createOptions(config.raft);
}
