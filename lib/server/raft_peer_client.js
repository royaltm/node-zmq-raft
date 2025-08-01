/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const debug = require('debug')('zmq-raft:peer-client');

const { decode: decodeMsgPack } = require('@royaltm/msgpack-lite');

const ZmqRaftClient = require('../client/zmq_raft_client');

const { assertConstantsDefined } = require('../utils/helpers');

const { lastConfigOffsetOf, readers: { readDataOf, readRequestIdOf } } = require('../common/log_entry');

const getConfigEntryPeers = (entry) => (entry && decodeMsgPack(readDataOf(entry)));
const getConfigEntryRequestKey = (entry) => (entry && readRequestIdOf(entry, 'base64'));

const { FSM_CLIENT } = require('../common/constants');

assertConstantsDefined({ FSM_CLIENT }, 'symbol');

exports.synchronizeLogEntries = function() {
  const lastApplied = this.lastApplied
      , log = this._log;

  debug('synchronizing log with the cluster after index: %s', lastApplied);

  const client = new ZmqRaftClient(Array.from(this.peers.values()), {secret: this._secretBuf});

  const close = () => client.destroy();

  return client.requestConfig()
  .then(() => new Promise((resolve, reject) => {
    var logwriter = log.createLogEntryWriteStream();

    client.requestEntriesStream(lastApplied)
    .on('error', reject)
    .on('data', chunk => {
      var peersIndex = chunk.logIndex;
      if (chunk.isConfigEntry && this.peersIndex < peersIndex) {
        logwriter.cork();
        var peers = getConfigEntryPeers(chunk)
          , peersUpdateRequest = getConfigEntryRequestKey(chunk);
        this._updateConfig(peers, peersUpdateRequest, peersIndex);
        this._persistence.update({peers, peersUpdateRequest, peersIndex})
        .then(() => logwriter.uncork(), (err) => reject(err))
      }
    })
    .pipe(logwriter)
    .on('error', reject)
    .on('finish', () => resolve(logwriter.commit()));
  }))
  .then(() => {
    debug('done synchronizing log, lastIndex: %s lastTerm: %s', log.lastIndex, log.lastTerm);
    var currentTerm = this.currentTerm;
    if (currentTerm < log.lastTerm) currentTerm = log.lastTerm;
    return Promise.all([
      this._updateState(FSM_CLIENT, currentTerm, this.votedFor),
      client.waitForQueues(5000).then(close, close)
    ]);
  })
  .catch(err => { close(); throw err; });
};
