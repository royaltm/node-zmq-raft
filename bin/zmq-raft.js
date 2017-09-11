#!/usr/bin/env node
/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
 if (require.main !== module) throw new Error("zmq-raft.js must be run directly from node");

const path = require('path')
    , fs = require('fs');

const program = require('commander')
    , Hjson = require('hjson')
    , debug = require('debug')('zmq-raft');

const package = require('../package.json');

const raft = require('..');

const defaultConfig = path.join(__dirname, '..', 'config', 'default.hjson');

program
  .version(package.version)
  .usage('[options] [id]')
  .option('-c, --config <file>', 'Config file', defaultConfig)
  .parse(process.argv);

const config = readConfig(program.config);

if (program.args.length >= 1) {
  config.raft.id = program.args[0];
}

const peerId = config.raft.id
    , peers = config.raft.peers;

var myPeer = peers.find(peer => peer.id === peerId);

if (myPeer) {
  if (myPeer.pub) {
    config.raft.broadcast || (config.raft.broadcast = {});
    config.raft.broadcast.url = myPeer.pub;
  }
}

raft.server.build(config.raft)
.then(zmqRaft => {

  zmqRaft.on('error', (err) => {
    console.error("RAFT ERROR");
    console.error(err.stack);
    zmqRaft && zmqRaft.close().then(() => process.exit(1), () => process.exit(2));
  });

  process
  .on('SIGUSR2', () => {
    debug('terminating * SIGUSR2 *');
    shutdown();
  })
  .on('SIGTERM', () => {
    debug('terminating * SIGINT *');
    shutdown();
  })
  .on('SIGINT', () => {
    debug('terminating * SIGINT *');
    shutdown();
  })
  .on('SIGHUP', () => {
    debug('terminating * SIGHUP *');
    shutdown();
  })
  .on('uncaughtException', (err) => {
    debug('Caught unhandled exception: %s', err);
    if (err.stack) console.error(err.stack);
    shutdown();
  })
  .on('exit', () => {
    console.log('bye bye');
  });

  function shutdown() {
    if (!zmqRaft) process.exit();
    zmqRaft.close().then(() => process.exit(), () => process.exit());
    zmqRaft = undefined;
  }

  console.log('server listening at: %s', zmqRaft.url);
})
.catch(err => {
  console.error("RAFT INIT ERROR");
  console.error(err.stack);
  process.exit();
});

function readConfig(configFile) {
  const text = fs.readFileSync(configFile, 'utf8');
  const config = Hjson.parse(text);
  if ('object' !== typeof config.raft || config.raft === null) {
    config.raft = {};
  }
  if (!Array.isArray(config.raft.peers)) config.raft.peers = [];
  return config;
}
