/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray
    , MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;

const path = require('path');

const FilePersistence = require('../common/file_persistence');

const { assertConstantsDefined } = require('../utils/helpers');

const { REQUEST_LOG_ENTRY_BASE64_LENGTH } = require('../common/log_entry');

assertConstantsDefined({ REQUEST_LOG_ENTRY_BASE64_LENGTH }, 'number');

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
    super(filename, {currentTerm: 0, votedFor: null, peers: initPeers, peersUpdateRequest: null, peersIndex: null});
  }

  [Symbol.for('apply')]({currentTerm, votedFor, peers, peersUpdateRequest, peersIndex}) {
    if (currentTerm !== undefined) this.currentTerm = currentTerm;
    if (votedFor !== undefined) this.votedFor = votedFor;
    if (peers !== undefined) this.peers = peers;
    if (peersUpdateRequest !== undefined) this.peersUpdateRequest = peersUpdateRequest;
    if (peersIndex !== undefined) this.peersIndex = peersIndex;
  }

  [Symbol.for('validate')]({currentTerm, votedFor, peers, peersUpdateRequest, peersIndex}, withAllProperties) {
    var data = {};

    if (currentTerm !== undefined) data.currentTerm = validateCurrentTerm(currentTerm);
    else if (withAllProperties) {
      data.currentTerm = this.currentTerm;
    }

    if (votedFor !== undefined) data.votedFor = validateVotedFor(votedFor);
    else if (withAllProperties) {
      data.votedFor = this.votedFor;
    }

    if (peers !== undefined) data.peers = validatePeers(peers);
    else if (withAllProperties) {
      data.peers = this.peers;
    }

    if (peersUpdateRequest !== undefined) data.peersUpdateRequest = validatePeersRequest(peersUpdateRequest);
    else if (withAllProperties) {
      data.peersUpdateRequest = this.peersUpdateRequest;
    }

    if (peersIndex !== undefined) data.peersIndex = validatePeersIndex(peersIndex);
    else if (withAllProperties) {
      data.peersIndex = this.peersIndex;
    }

    return data;
  }
}

module.exports = exports = RaftPersistence;

function validateCurrentTerm(currentTerm) {
  if (!Number.isFinite(currentTerm) || currentTerm % 1 !== 0 ||
      currentTerm < 0 ||
      currentTerm > MAX_SAFE_INTEGER) throw new Error("RaftPersistence: invalid currentTerm");
  return currentTerm;
}

function validateVotedFor(votedFor) {
  if (votedFor !== null && 'string' !== typeof votedFor) throw new Error("RaftPersistence: invalid votedFor");
  return votedFor;
}

function validatePeers(peers) {
  if (!isArray(peers) &&
      (peers === null || 'object' !== typeof peers || !isArray(peers.old) || !isArray(peers.new))) {
    throw new Error("RaftPersistence: invalid peers");
  }
  return peers;
}

function validatePeersRequest(peersUpdateRequest) {
  if (peersUpdateRequest !== null &&
    ('string' !== typeof peersUpdateRequest || peersUpdateRequest.length !== REQUEST_LOG_ENTRY_BASE64_LENGTH)) {
    throw new Error("RaftPersistence: invalid peersUpdateRequest");
  }
  return peersUpdateRequest;
}

function validatePeersIndex(peersIndex) {
  if (peersIndex !== null && (!Number.isFinite(peersIndex) || peersIndex % 1 !== 0)) {
    throw new Error("RaftPersistence: invalid peersIndex");
  }
  return peersIndex;
}
