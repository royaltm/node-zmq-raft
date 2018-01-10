/*
 *  Copyright (c) 2016-2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const assert = require('assert');

const colors = require('colors/safe')
    , { cyan, green, grey, magenta, red, yellow, bgGreen } = colors;

const ZmqRaftClient = require('../client/zmq_raft_client');
const { lpad } = require('./helpers');

exports.listPeers = function listPeers(client) {
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
};

exports.showInfo = function showInfo(client, id) {
  if (!client) {
    console.log(yellow('not connected'));
    return Promise.resolve();
  }
  var cleanUp, anyPeer = false;
  if (client.peers.has(id)) {
    client = new ZmqRaftClient({peers: [[id, client.peers.get(id)]]});
    cleanUp = () => client.close();
    anyPeer = true;
  }
  else if (id) {
    console.log(yellow('unknown id: %s'), id);
    return Promise.resolve();
  }
  else {
    cleanUp = () => {};
  }

  return client.requestLogInfo(anyPeer, 5000)
  .then(({isLeader, leaderId, currentTerm, firstIndex, lastApplied, commitIndex, lastIndex, snapshotSize, pruneIndex}) => {
    if (!anyPeer) assert(isLeader);
    console.log(grey(`Log information for: "${isLeader ? green(leaderId) : yellow(id)}"`));
    console.log(`leader:          ${isLeader ? green('yes') : cyan('no')}`);
    console.log(`current term:    ${magenta(lpad(currentTerm, 14))}`);
    console.log(`first log index: ${magenta(lpad(firstIndex, 14))}`);
    console.log(`last applied:    ${magenta(lpad(lastApplied, 14))}`);
    console.log(`commit index:    ${magenta(lpad(commitIndex, 14))}`);
    console.log(`last log index:  ${magenta(lpad(lastIndex, 14))}`);
    console.log(`snapshot size:   ${magenta(lpad(snapshotSize, 14))}`);
    console.log(`prune index:     ${magenta(lpad(pruneIndex, 14))}`);
  })
  .then(cleanUp, err => { cleanUp(); throw err; });
};

exports.argToBoolean = function argToBoolean(arg) {
  switch(arg.toLowerCase()) {
  case 'yes':
  case 'y':
  case 'on':
  case '1':
    return true;
  default:
    return false;
  }
};

exports.prompt = function prompt(repl) {
  repl.lineParser && repl.lineParser.reset();
  if (repl.clearBufferedCommand) {
    repl.clearBufferedCommand();
  } else {
    repl.bufferedCommand = '';
  }
  repl.displayPrompt();
};

exports.replError = function replError(repl, err) {
  console.warn(red('ERROR'));
  console.warn(err.stack);
  prompt(repl);
};
