#!/usr/bin/env node
"use strict";
/*
 * BIN console
 *
 * Author: Rafal Michalski (c) 2017
 */

const assert = require('assert');
const os = require('os');
const dns = require('dns');
const fs = require('fs');
const path = require('path');
const util = require('util');

const isArray = Array.isArray;

const pkg = require('../package.json');

const tmpdir = path.normalize(path.join(__dirname, '..', 'tmp'));

const ben = require('ben');

const colors = require('colors/safe')
    , { cyan, green, grey, magenta, red, yellow, bgGreen } = colors;

const msgpack = require('msgpack-lite');

const lookup = require('../lib/utils/dns_lookup').hostsToZmqUrls;

const { createRepl } = require('../lib/utils/repl');
const ZmqRaftClient = require('../lib/client/zmq_raft_client');
const ZmqRaftSubscriber = require('../lib/client/zmq_raft_subscriber');
const { lpad, regexpEscape } = require('../lib/utils/helpers');
const { createRotateName } = require('../lib/utils/filerotate');

const argv = process.argv.slice(2);

const motd = util.format("%s REPL, welcome!", pkg.name);

console.log(grey("-".repeat(motd.length)));
console.log(motd);
console.log("version: %s", cyan(pkg.version));
console.log(grey("-".repeat(motd.length)));

const dbs = new Map();
var currentdb;

var subs, client;

function getClient() {
  return subs ? subs.client : client;
}

createRepl().then(repl => {
  repl.on('reset', initializeContext);
  repl.on('exitsafe', () => {
    console.log('Received "exit" event from repl!');
    process.exit();
  });
  repl.defineCommand('peers', {
    help: 'Show db remote cluster config',
    action: function() {
      listPeers(getClient()).then(() => prompt(repl)).catch(err => error(err));
    }
  });
  repl.defineCommand('info', {
    help: 'Show db remote log information of a specified peer or any peer',
    action: function(id) {
      showInfo(getClient(), id).then(() => prompt(repl)).catch(err => error(err));
    }
  });
  repl.defineCommand('connect', {
    help: 'Connect client to zmq-raft servers: host [host...]',
    action: function(hosts) {
      lookup(hosts.split(/\s+/)).then(urls => {
        client && client.close();
        client = new ZmqRaftClient(urls);
        repl.context.client = client;
        console.log('connecting to: %s', urls.join(', '));
      })
      .then(() => prompt(repl)).catch(err => error(err));
    }
  });
  repl.defineCommand('subscribe', {
    help: 'Subscribe to zmq-raft broadcast state servers: host[:port] [host...]',
    action: function(hosts) {
      lookup(hosts.split(/\s+/)).then(urls => {
        subs && subs.close();
        client && client.close();
        subs = new ZmqRaftSubscriber(urls);
        repl.context.client = subs.client;
        repl.context.subs = subs;
        console.log('connecting to: %s', urls.join(', '));
      })
      .then(() => prompt(repl)).catch(err => error(err));
    }
  });
  repl.defineCommand('close', {
    help: 'Subscribe to zmq-raft broadcast state servers: host[:port] [host...]',
    action: function(hosts) {
      subs && subs.close();
      client && client.close();
      repl.context.client = undefined;
      repl.context.subs = undefined;
      console.log('closed');
      prompt(repl);
    }
  });

  initializeContext(repl.context);

  function initializeContext(context) {

    Object.assign(context, {
      colors
    , ri: repl
    , mp: msgpack
    , ben
    , pkg
    , lookup
    });
  }

  function error(err) {
    console.warn(red('ERROR'));
    console.warn(err.stack);  
    prompt(repl);
  }

}).catch(err => console.warn(err.stack));

function listPeers(client) {
  if (!client) {
    console.log(yellow('not connected'));
    return Promise.resolve();
  }
  return client.requestConfig(5000).then(peers => {
    console.log(magenta('Cluster peers:'))
    for(let id in peers.urls) {
      let url = peers.urls[id];
      if (id === peers.leaderId) {
        console.log(bgGreen(`${id}: ${url}`));
      }
      else
        console.log(`${cyan(id)}: ${grey(url)}`);
    }
  });
}

function showInfo(client, id) {
  if (!client) {
    console.log(yellow('not connected'));
    return Promise.resolve();
  }
  var urls, anyPeer = false;
  if (client.peers.has(id)) {
    urls = client.urls;
    client.setUrls(client.peers.get(id));
    anyPeer = true;
  }
  return client.requestLogInfo(anyPeer, 5000)
  .then(({isLeader, leaderId, currentTerm, firstIndex, lastApplied, commitIndex, lastIndex, snapshotSize}) => {
    if (!anyPeer) assert(isLeader);
    console.log(grey(`Log information for: "${isLeader ? green(leaderId) : yellow(id)}"`));
    console.log(`leader:          ${isLeader ? green('yes') : cyan('no')}`);
    console.log(`current term:    ${magenta(lpad(currentTerm, 14))}`);
    console.log(`first log index: ${magenta(lpad(firstIndex, 14))}`);
    console.log(`last applied:    ${magenta(lpad(lastApplied, 14))}`);
    console.log(`commit index:    ${magenta(lpad(commitIndex, 14))}`);
    console.log(`last log index:  ${magenta(lpad(lastIndex, 14))}`);
    console.log(`snapshot size:   ${magenta(lpad(snapshotSize, 14))}`);
    if (urls) client.setUrls(urls);
  });
}

function prompt(repl) {
  repl.lineParser.reset();
  repl.bufferedCommand = '';
  repl.displayPrompt();
};
