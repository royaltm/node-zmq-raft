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

/**
 * Implements the FilePersistence for RAFT state data.
 *
 * The properties being stored:
 *
 * - `currentTerm` {number} - RAFT related,
 * - `votedFor` {string|null} - RAFT related,
 * - `peers` {Array|Object} - An array of cluster peer ids and urls as tuples [id, url].
 *         Transitional peer configuration is instead an object with properties "old" and "new",
 *         each containing an array of peers in the cluster before and after the configuration transition.
 * - `peersUpdateRequest` {string|null} - a base64 encoded 12-byte request id of the `ConfigUpdate RPC`
 *         message that initiated the last configuration transition. If the current configuration is
 *         transitional and this property is `null`, the Raft server will refuse to start.
 *         This property may be `null` only before the first ever membership change, when the current
 *         cluster configuration is a SEED configuration.
 * - `peersIndex` {number|null} - a LOG INDEX of the RAFT log CONFIGURATION entry containing the current
 *         cluster peer membership state. If it's `null` then the configuration entry wasn't yet written
 *         to the log by the peer. There's a moment in time when the new configuration has been
 *         written to the RAFT state file by the TERM LEADER but the log entry is in the process of writing.
 *         This never happens on FOLLOWERS, as they receive the new configuration via a log entry.
 *         If the Raft server crashes in this state, when restarted, the server will refuse to start.
 *         The `builder` however tries to recover from this state by finding the log entry with the
 *         aforementioned `peersUpdateRequest` and update the state file before starting the server.
 *         If however the log entry couldn't be found, the peer's data should be scrapped anyway and
 *         recreated by the protocol.
**/
class RaftPersistence extends FilePersistence {

  /**
   * Creates a new instance of RaftPersistence.
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
