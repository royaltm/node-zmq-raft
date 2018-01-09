#!/usr/bin/env node
/* 
 *  Copyright (c) 2016-2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

if (require.main !== module) throw new Error("zmq-raft.js must be run directly from node");

const path = require('path');

const program = require('commander')
    , debug = require('debug')('zmq-raft');

const pkg = require('../package.json');

const raft = require('..');

const { readConfig } = require('../lib/utils/config');

const defaultConfig = path.join(__dirname, '..', 'config', 'default.hjson');

program
  .version(pkg.version)
  .usage('[options] [id]')
  .description('start zmq-raft cluster peer using provided config and optional id')
  .option('-c, --config <file>', 'config file', defaultConfig)
  .option('--ns [namespace]', 'raft config root namespace', 'raft')
  .parse(process.argv);

readConfig(program.config, program.ns).then(config => {
  if (program.args.length >= 1) {
    config.id = program.args[0];
  }

  const peerId = config.id
      , peers = config.peers;

  var myPeer = peers.find(peer => peer.id === peerId);

  if (myPeer) {
    if (myPeer.pub) {
      config.broadcast || (config.broadcast = {});
      config.broadcast.url = myPeer.pub;
    }
  }

  return raft.server.builder.build(config)
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

    console.log('server listening at: %s', zmqRaft.url || zmqRaft.routerBindUrl);
  });
})
.catch(err => {
  console.error("RAFT INIT ERROR");
  console.error(err.stack);
  process.exit();
});
