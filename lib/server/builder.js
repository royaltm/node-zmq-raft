/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray;

const path = require('path');

const debug = require('debug')('zmq-raft:builder');

const { ZmqRaft
      , FileLog
      , RaftPersistence 
      , BroadcastStateMachine
      } = require('.');

const { fsutil: { mkdirp }
      , helpers: { parsePeers, isNonEmptyString }
      } = require('../utils');

const defaultOptions = {
    /* required */
    id: "local"
  , secret: ""
  , peers: [
      {id: "local", url: "tcp://127.0.0.1:8047"}
  ]
  , data: {
      /* required */
      path: "raft" /* full path to raft base directory */
    , raft: "raft.pers" /* filename in data.path */
    , log: "log" /* directory name in data.path */
    , snapshot: "snap" /* filename in data.path */
    , state: "state.pers" /* filename in data.path */
    }
  , router: {
      /* optional */
      // bind: "tcp://*:8047"
  }
  , broadcast: {
      /* required for default broadcast state */
      url: "tcp://127.0.0.1:8048"
      /* optional */
      // bind: "tcp://*:8048"
  }
  , listeners: {
      error: (err) => {
        console.warn("RAFT ERROR");
        console.warn(err.stack);
        raft.close().then(() => process.exit(), () => process.exit());
      }
    , state: (state, currentTerm) => {
        debug('raft-state: %s term: %s', state, currentTerm);
      }
    , close: () => {
        debug('raft closed');
      }
  }
  , factory: {
      persistence: createRaftPersistence
    , log: createFileLog
    , state: createBroadcastStateMachine
  }
  /* optional */
  //, electionTimeoutMin: ELECTION_TIMEOUT_MIN,
  //, electionTimeoutMax: ELECTION_TIMEOUT_MAX,
};


function createFileLog(options) {
  const raftdir = options.data.path
      , logdir = path.join(raftdir, options.data.log)
      , snapfile = path.join(raftdir, options.data.snapshot);

  return new FileLog(logdir, snapfile);
}

function createRaftPersistence(options) {
  const raftdir = options.data.path
      , filename = path.join(raftdir, options.data.raft);

  return new RaftPersistence(filename, options.peers);
}

function createBroadcastStateMachine(options) {
  const raftdir = options.data.path
      , url = options.broadcast.url
      , filename = path.join(raftdir, options.data.state);

  return new BroadcastStateMachine(filename, url, {
    secret: options.secret,
    bindUrl: options.broadcast.bind
  });
}

module.exports = exports = function build(options) {
  debug('building raft server');

  try {
    options = createOptions(options, defaultOptions);

    if (!isNonEmptyString(options.id)) {
      throw new Error("raft builder: id must be a non-empty string");
    }
    if ('string' !== typeof options.secret) {
      throw new Error("raft builder: secret must be a string");
    }
    if (!isNonEmptyString(options.data.path)) {
      throw new Error("raft builder: data.path must be a non-empty string");
    }
    if (options.peers.length === 0) {
      throw new Error("raft builder: initial peers must be non-empty");
    }
    if (options.router.bind !== undefined && !isNonEmptyString(options.router.bind)) {
      throw new Error("raft builder: router.bind must be a non-empty string");
    }
  } catch(err) {
    return Promise.reject(err);
  }

  debug('ensuring directory data.path: %j', options.data.path);
  return mkdirp(options.data.path).then(() => {

    debug('initializing persistence');
    const persistence = options.factory.persistence(options);

    debug('initializing log');
    const log = options.factory.log(options);

    debug('initializing state machine');
    const stateMachine = options.factory.state(options);

    return Promise.all([
        persistence.ready()
      , log.ready()
      , stateMachine.ready()
      ]);
  })

  .then(([persistence, log, stateMachine]) => {

    const updatePersist = {};
    // if (!peersEquals(persistence.peers, peers)) {
    //   debug('updating peers in persistence');
    //   updatePersist.peers = peers;
    // }

    if (persistence.currentTerm === 0 && log.lastTerm > 0) {
      debug("updating raft current term to log's last term: %s -> %s", persistence.currentTerm, log.lastTerm);
      updatePersist.currentTerm = log.lastTerm;
    }

    if (updatePersist.peers !== undefined || updatePersist.currentTerm !== undefined) {
      return persistence.rotate(updatePersist).then(() => [persistence, log, stateMachine]);
    }
    else return [persistence, log, stateMachine];
  })

  .then(([persistence, log, stateMachine]) => {

    const {id, secret, electionTimeoutMin, electionTimeoutMax, router: {bind}} = options;

    debug('initializing raft: %j', id);
    const raft = new ZmqRaft(id, persistence, log, stateMachine,
                              {bindUrl: bind, electionTimeoutMin, electionTimeoutMax});

    for(let event of Object.keys(options.listeners)) {
      const handler = options.listeners[event];
      if ('function' === typeof handler) {
        raft.on(event, handler);
      }
    }

    return raft.ready();
  });

};

function createOptions(options, defaults) {
  options = Object.assign({}, options);
  for(let name of Object.keys(defaults)) {
    let defval = defaults[name]
      , value = options[name];

    if ('object' === typeof defval && defval !== null && !isArray(defval)) {
      if ('object' === typeof value && value !== null && !isArray(value)) {
        options[name] = createOptions(value, defval);
      }
      else options[name] = defval;
    }
    else if (value === undefined) {
      options[name] = defval;
    }
  }
  return options;
}
