"use strict";

const assert = require('assert');
const os = require('os');
const fs = require('fs');
const path = require('path');
const dns = require('dns');
const cluster = require('cluster');
const util = require('util');

const colors = require('colors/safe');
const mkdirp = require('mkdirp');

const Raft = require('../lib/server/raft');
const FileLog = require('../lib/server/filelog');
const BroadcastStateMachine = require('../lib/server/broadcast_state_machine');
const RaftPersistence = require('../lib/server/raft_persistence');
const tmpdir = path.resolve(__dirname, '..', 'tmp');

const argv = process.argv.slice(2);

if (argv.length > 2) {
  var hosts = argv;
}
else {
  var [numpeers, id] = argv;
}

numpeers>>>=0
id>>>=0

var port = (process.env.PORT || 8000) & 0xffff;
var dir;

resolve().then(host => {
  if (hosts && hosts.length) {
    return Promise.all([host].concat(hosts.map(host => resolve(host))));
  }
  else return [host];
}).then(([me, ...peers]) => {
  if (peers.length === 0) {
    return (id) => {
      return {id: (id + 100).toString().substr(1), url: `tcp://${me}:${port+id}`, pub: `tcp://${me}:${port+100+id}`};
    };
    dir = path.join(tmpdir, (id + 100).toString().substr(1));
  }
  else if (peers.includes(me)) {
    numpeers = peers.length;
    id = peers.indexOf(me) + 1;
    return (id) => {
      return {id: (id + 100).toString().substr(1), url: `tcp://${peers[id - 1]}:${port}`, pub: `tcp://${peers[id - 1]}:${port+100}`};
    };
    dir = path.join(tmpdir, '00');
  }
  else throw new Error('peers without us');
}).then(genpeer => {
  assert(numpeers > 0 && numpeers <= 100);
  assert(id > 0 && id <= numpeers);

  var options = {secret: process.env.SECRET || 'kiełbasa'};
  var me = genpeer(id);
  var myId = me.id;
  var peers = [];
  for(let i = 0; i < numpeers; ++i) peers.push(genpeer(i + 1));

  for(let peer of peers) {
    let url = peer.url;
    if (peer.id === myId) {
      console.log(colors.green(`${peer.id}: ${url}`));
    }
    else
      console.log(`${colors.cyan(peer.id)}: ${colors.grey(url)}`);
  }

  mkdirp.sync(dir);

  var persistence = new RaftPersistence(path.join(dir, 'raft.pers'), peers);
  var log = new FileLog(path.join(dir, 'log'), path.join(dir, 'snap'));
  var stateMachine = new BroadcastStateMachine(path.join(dir, 'state.pers'), me.pub, options)

  return Promise.all([log.ready(),stateMachine.ready(),persistence.ready()]).then(() => {
    var promises = [];
    if (log.firstIndex > stateMachine.lastApplied + 1) {
      console.warn(colors.yellow("UPDATING STATE MACHINE: %s -> %s"), stateMachine.lastApplied, log.firstIndex - 1);
      promises.push(stateMachine.rotate({lastApplied: log.firstIndex - 1}));
    }
    else promises.push(stateMachine.rotate({}));
    if (log.lastTerm > persistence.currentTerm) {
      console.warn(colors.yellow("UPDATING CURRENT TERM: %s -> %s"), persistence.currentTerm, log.lastTerm);
      promises.push(persistence.rotate({currentTerm: log.lastTerm}));
    }
    else promises.push(persistence.rotate({}));
    return Promise.all(promises);
  }).then(() => {
    var raft = new Raft(myId, persistence, log, stateMachine, options);

    raft.on('error', err => {
      console.warn(colors.bgRed("RAFT ERROR"));
      console.warn(err.stack);
      process.exit();
    });
    raft.on('state', (state, currentTerm) => {
      console.log('state: %s term: %s', colors.bgGreen.black(state), colors.cyan(currentTerm));
    });

    return Promise.all([raft.ready()]).then(raft => {
      console.log(colors.rainbow('WEEEHAAA!'));
      // console.log(raft);
    });
  });
}).catch(err => {
  console.warn(err.stack)
});



function resolve(hostname) {
  return new Promise((resolve, reject) => {
    dns.lookup(hostname || os.hostname(), (err, address, family) => {
      if (err) return reject(err);
      resolve(family == 4 ? address : `[${address}]`);
    });
  });
}
