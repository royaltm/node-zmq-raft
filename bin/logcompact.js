#!/usr/bin/env node
/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
 if (require.main !== module) throw new Error("logcompact.js must be run directly from node");

const path = require('path')
    , fs = require('fs')
    , assert = require('assert');

const { Z_BEST_COMPRESSION } = require('zlib');

const program = require('commander')
    , Hjson = require('hjson')
    , debug = require('debug')('zmq-raft:logcompact');

const package = require('../package.json');

const raft = require('..');

const { server: { FileLog, builder: { createOptions } }
      , common: { SnapshotFile }
      , client: { ZmqRaftClient }
      , utils: { fsutil: { mkdirp }, tempfiles: { createTempName, cleanupTempFiles } }
      } = raft;

const defaultConfig = path.join(__dirname, '..', 'config', 'default.hjson');

program
  .version(package.version)
  .usage('[options]')
  .option('-c, --config <file>', 'Config file', defaultConfig)
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

if (!isFinite(program.zip)) program.zip = Z_BEST_COMPRESSION;

const options = readConfig(program.config);

const rootDir = program.dir || options.data.path;
const logDir = path.resolve(rootDir, program.log || options.data.log || 'log');
const snapFile = path.resolve(rootDir, program.snapshot || options.data.snapshot || 'snap');

const targetFile = program.target ||
                   (options.data.compact.install && path.resolve(rootDir, options.data.compact.install));
const stateMachinePath = program.stateMachine ||
                         (options.data.compact.state.path && path.resolve(rootDir, options.data.compact.state.path));

const lastIndex = program.index;
const peerUrl = program.peer; // TODO peer id

if (!targetFile) {
  console.error("no target file");
  process.exit(2);
}

if (!stateMachinePath) {
  console.error("no state machine specified");
  process.exit(3);
}

var lastIndexPromise;
if (isFinite(lastIndex) && lastIndex > 0) {
  lastIndexPromise = Promise.resolve(lastIndex);
}
else if (peerUrl) {
  const client = new ZmqRaftClient({peers: [peerUrl]});
  lastIndexPromise = client.requestLogInfo(true, 5000).then(({commitIndex}) => {
    client.close();
    return commitIndex;
  });
}
else {
  console.error("specify last index or peer url");
  process.exit(4);
}

const StateMachineClass = loadState(stateMachinePath);

const stateMachine = new StateMachineClass(Object.assign({}, options.data.compact.state.options, {
  compressionLevel: program.zip,
  unzipSnapshot: program.unzip
}));
const fileLog = new FileLog(logDir, snapFile, true);

Promise.all([
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
      const snapshotData = stateMachine.serialize();
      tempSnapshot = new SnapshotFile(createTempName(targetFile), lastIndex, lastTerm, snapshotData.length);
      tempSnapshotPromise = tempSnapshot.ready().then(s => s.write(snapshotData, 0, snapshotData.length));
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
.then(() => debug('compaction done'))
.catch(err => {
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
    const text = fs.readFileSync(configFile, 'utf8');
    config = Hjson.parse(text);
  }
  else config = {};

  return createOptions(config.raft);
}
