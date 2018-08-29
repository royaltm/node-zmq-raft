/* 
 *  Copyright (c) 2016-2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray;

const path = require('path');

const { Z_BEST_COMPRESSION } = require('zlib');

const debug = require('debug')('zmq-raft:builder');

const { ZmqRaft
      , FileLog
      , RaftPersistence 
      , BroadcastStateMachine
      } = require('.');

const { fsutil: { mkdirp }
      , helpers: { parsePeers, isNonEmptyString, createOptionsFactory }
      } = require('../utils');

const WebMonitor = require('./webmonitor');

const defaultOptions = exports.defaultOptions = {
    /* required */
  // id: "local"
    secret: ""
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
    , compact: {
        /* optional */
        install: "compact/snap.new"
      , watch: false
      , state:
      {
          /* used by logcompaction */
          // path: "../example/passthrough_state"
          options: {
            compressionLevel: Z_BEST_COMPRESSION
            // unzipSnapshot: true
          }
        }
      }
    /* optional */
    , appendIdToPath: false
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
      error: null
    , config: null
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
  , webmonitor: {
      enable: false,
      port: 8050,
      bind: "::"
  }
  /* optional */
  //, electionTimeoutMin: ELECTION_TIMEOUT_MIN
  //, electionTimeoutMax: ELECTION_TIMEOUT_MAX
  //, maxLogEntryDataSize: MAX_LOG_ENTRY_DATA_SIZE
  //, peerMsgDataSize: PEER_MSG_DATA_SIZE
};

const createOptions = exports.createOptions = createOptionsFactory(defaultOptions);

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

exports.build = function build(options) {
  debug('building raft server');

  try {
    options = createOptions(options);

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
    if (options.data.appendIdToPath) {
      options.data.path = path.join(options.data.path, options.id);
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

    const logPromise = options.data.compact.watch
                     ? log.ready()
                       .then(log => log.watchInstallSnapshot(path.join(options.data.path, options.data.compact.install)))
                       .then(() => log)
                     : log.ready();

    return Promise.all([
        persistence.ready()
      , logPromise
      , stateMachine.ready()
      ]);
  })

  .then(([persistence, log, stateMachine]) => {

    const updatePersist = {};
    // if (!peersEquals(persistence.peers, peers)) {
    //   debug('updating peers in persistence');
    //   updatePersist.peers = peers;
    // }

    if (persistence.currentTerm < log.lastTerm) {
      debug("updating raft current term to log's last term: %s -> %s", persistence.currentTerm, log.lastTerm);
      updatePersist.currentTerm = log.lastTerm;
    }

    if (persistence.peersUpdateRequest != null && persistence.peersIndex == null) {
      let peersIndex = log.getRid(persistence.peersUpdateRequest);
      if (peersIndex !== undefined) {
        debug("confirming peersIndex: %s of: [%s] in persistence", peersIndex, persistence.peersUpdateRequest);
        updatePersist.peersIndex = peersIndex;
      }
    }

    if (Object.keys(updatePersist).length !== 0) {
      return persistence.rotate(updatePersist).then(() => [persistence, log, stateMachine]);
    }
    else return [persistence, log, stateMachine];
  })

  .then(([persistence, log, stateMachine]) => {
    if (log.firstIndex > stateMachine.lastApplied + 1) {
      debug("feeding state machine with a snapshot: %s -> %s term: %s", stateMachine.lastApplied, log.snapshot.logIndex, persistence.currentTerm);
      return log.feedStateMachine(stateMachine, log.snapshot.logIndex, persistence.currentTerm)
                .then(() => [persistence, log, stateMachine]);
    }
    else return [persistence, log, stateMachine];
  })

  .then(([persistence, log, stateMachine]) => {

    const { id, secret
          , electionTimeoutMin, electionTimeoutMax
          , maxLogEntryDataSize
          , peerMsgDataSize
          , router: {bind}
          , webmonitor } = options;

    debug('initializing raft: %j', id);
    if (electionTimeoutMin !== undefined) debug('election timeout min.: %j ms', electionTimeoutMin);
    if (electionTimeoutMax !== undefined) debug('election timeout max.: %j ms', electionTimeoutMax);
    if (maxLogEntryDataSize !== undefined) debug('maxmimum log entry date size: %j bytes', maxLogEntryDataSize);
    if (peerMsgDataSize !== undefined) debug('peer message data size: %j bytes', peerMsgDataSize);

    const raft = new ZmqRaft(id, persistence, log, stateMachine,
                              { bindUrl: bind, secret
                              , electionTimeoutMin, electionTimeoutMax
                              , maxLogEntryDataSize
                              , peerMsgDataSize
                              });
    for(let event of Object.keys(options.listeners)) {
      const handler = options.listeners[event];
      if ('function' === typeof handler) {
        raft.on(event, handler);
      }
    }

    if (webmonitor.enable) {
      raft.webmonitor = new WebMonitor(raft, webmonitor);
    }

    return raft.ready();
  });

};
