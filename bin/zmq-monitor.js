#!/usr/bin/env node
"use strict";

if (require.main !== module) throw new Error("zmq-monitor.js must be run directly from node");

const path = require('path');
const parseUrl = require('url').parse;
const { format } = require('util');

const { cyan, green, grey, magenta, red, yellow, bgGreen } = require('colors/safe');

const program = require('commander')
    , debug = require('debug')('zmq-monitor');

const raft = require('..');
const { lpad
      , validateIntegerOption } = raft.utils.helpers;

program
  .version('1.0.0')
  .usage('[options] url...')
  .description('start zmq-monitor using provided options and url seeds')
  .option('-k, --cluster <secret>', 'Secret cluster identity part of the protocol', '')
  .option('-i, --interval <secs>', 'How often peers should be queried (in seconds)')
  .option('-t, --timeout <secs>', 'How long to wait for a peer to respond (in seconds)')
  .parse(process.argv);

if (program.args.length === 0) {
  console.error("zmq-monitor: please provide at least one cluster seed url")
  process.exit(1);
}

const options = {
    // timeout: 5000,
    // serverElectionGraceDelay: 15000,
    secret: program.cluster,
    urls: program.args
};

try {
  function validateSeconds(name, min) {
    if (name in program) {
      let value = parseFloat(program[name]);
      if (!Number.isFinite(value)) throw new TypeError(`${name} must be a number of seconds`);
      if (min !== undefined && value < min) throw new TypeError(`${name} must be >= ${min}`);
      return value * 1000;
    }
  }

  let timeoutMs = validateSeconds('timeout', 1);
  if (timeoutMs != null) {
    options.timeout = timeoutMs;
    options.serverElectionGraceDelay = timeoutMs;
    options.requestInfoTimeout = timeoutMs;
    options.requestConfigTimeout = timeoutMs * 4;
  }

  let intervalMs = validateSeconds('interval', 0.1);
  if (intervalMs != null) {
    options.requestInfoInterval = intervalMs;
    options.queryDelayFirst = intervalMs / 10;
    options.queryDelayStep = intervalMs / 5;
  }
}
catch(err) {
  console.error("zmq-monitor: %s", err)
  process.exit(1);  
}

const mon = new raft.utils.monitor.ZmqRaftMonitor(options);
const peerInfo = new Map();

mon
.on('peers', (peers, leaderId) => {
  console.log('leaderId: %s', leaderId);
  for(let id in peers) {
    console.log('%s: %s', id, peers[id]);
    peerInfo.set(id, {
      id,
      url: peers[id],
      info: {}
    });
  }
  printAll();
})
.on('info', (id, info) => {
  peerInfo.get(id).info = info;
  printAll();
})
.on('peer-error', (id, err) => {
  peerInfo.get(id).info.err = err;
  printAll();
})
.on('error', err => {
  console.error('error: %s', err);
})
.on('close', () => debug('closed'))

function fval(value, size, padder) {
  value = (value == null) ? '?' : '' + value;
  return lpad('' + value, size, padder);
}

function printAll() {
   // isLeader {bool}
   // leaderId {string|null}
   // currentTerm {number}
   // firstIndex {number}
   // lastApplied {number}
   // commitIndex {number}
   // lastIndex {number}
   // snapshotSize {number}
   // pruneIndex {number}

  console.clear();
  console.log('%s:   %s',
          fval('peer ID', 10, ' '),

          fval('term', 8, ' '),
          fval('commit', 8, ' '),
          fval('last', 8, ' '),
          // fval('prune', 8, ' '),
          fval('snap', 8, ' '),
          'url',
  );
  for(let {id, url, info} of peerInfo.values()) {
    let line = format('%s: %s %s',
                          fval(id, 10, ' '),
                          info.err ? 'X' : (info.isLeader ? 'M' : 'F'),
                          fval(info.currentTerm, 8, ' '),
                          fval(info.commitIndex, 8, ' '),
                          fval(info.lastIndex, 8, ' '),
                          // fval(info.pruneIndex, 8, ' '),
                          fval(info.snapshotSize, 8, ' '),
                          url);
    if (info.err) {
      console.log(red(line));
    }
    else if (info.isLeader) {
      console.log(bgGreen(line));
    }
    else {
      console.log(line);
    }
  }
}