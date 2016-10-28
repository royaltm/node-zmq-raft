/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray
    , MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;

const path = require('path');

const FilePersistence = require('../common/file_persistence');

const debug = require('debug')('raft-persistence');


class RaftPersistence extends FilePersistence {

  /**
   * Creates new instance
   *
   * `filename` should contain path to a filename in some existing directory.
   * `initPeers` should contain initial peers configuration.
   *
   * A new file will be created if `filename` does not exist.
   *
   * @param {string} filename
   * @param {Array} initPeers
   * @return this
  **/
  constructor(filename, initPeers) {
    super(filename, {currentTerm: 0, votedFor: null, peers: initPeers});
  }

  [Symbol.for('apply')]({currentTerm, votedFor, peers}) {
    if (currentTerm !== undefined) this.currentTerm = currentTerm;
    if (votedFor !== undefined) this.votedFor = votedFor;
    if (peers !== undefined) this.peers = peers;
  }

  [Symbol.for('validate')]({currentTerm, votedFor, peers}, withAllProperties) {
    var data = {};

    if (currentTerm !== undefined) data.currentTerm = validateCurrentTerm(currentTerm);
    else if (withAllProperties) {
      data.currentTerm = this.currentTerm;
    }

    if (votedFor !== undefined) data.votedFor = validateVotedFor(votedFor);
    else if (withAllProperties) {
      data.votedFor = this.votedFor;
    }

    if (peers !== undefined) data.peers= validatePeers(peers);
    else if (withAllProperties) {
      data.peers = this.peers;
    }

    return data;
  }
}

module.exports = exports = RaftPersistence;

function validateCurrentTerm(currentTerm) {
  if ('number' !== typeof currentTerm ||
      currentTerm < 0 ||
      currentTerm > MAX_SAFE_INTEGER) throw new Error("RaftPersistence: invalid currentTerm");
  return currentTerm;
}

function validateVotedFor(votedFor) {
  if (votedFor !== null && 'string' !== typeof votedFor) throw new Error("RaftPersistence: invalid votedFor");
  return votedFor;
}

function validatePeers(peers) {
  if (!isArray(peers)) throw new Error("RaftPersistence: invalid peers");
  return peers;
}
