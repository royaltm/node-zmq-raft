/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray;
const { mergeMaps, majorityOf, parsePeers } = require('../utils/helpers');

/*

  cfg = new Config(myPeerId, [[]]|{old:[], new:[]})

  @property {string} peerId

  @property {boolean} isTransitional
  @property {boolean} isSoleMaster

  @property {Map} ocluster
  @property {Map} ncluster

  @property {number} majority
  @property {number} ncMajority

  @property {Set} voted
  @property {Set} ncVoted
  @property {number} votes
  @property {number} ncVotes

  @property {Map} peers (a single/joint config peers map without myPeerId) - save to share

  @property {Array} peersAry -> (a single/joint config peers array)

  cfg.getUrl(peerId) -> string
  cfg.join(cfg) -> true/false
  cfg.replace(cfg)
  cfg.serialize();
  cfg.serializeNC();

  cfg.votingStart()
  cfg.vote(peerId, voteGranted)
  cfg.majorityHasVoted()
  cfg.hasWonVoting()
  cfg.majorityHasIndex(index, matchIndex)

  cfg.createOtherPeersMap(fctory)
  cfg.resetOtherPeersMap(map, value)
  cfg.updateOtherPeersMap(map, factory, destructor)

*/


class ClusterConfiguration {
  constructor(myPeerId, cfg) {
    this.peerId = myPeerId;
    this.peers = new Map();
    this.voted = new Set();
    this.votes = 0;
    this.ncVoted = new Set();
    this.ncVotes = 0;
    this.replace(cfg);
  }

  getUrl(peerId) {
    return this.ocluster.get(peerId) || this.ncluster.get(peerId);
  }

  get isTransitional() {
    return this.ncMajority !== 0;
  }

  get isSoleMaster() {
    return this.majority + this.ncMajority === 1;
  }

  serialize() {
    var ocluster = Array.from(this.ocluster)
      , ncluster = this.ncluster;

    return ncluster.size === 0 ? ocluster
                               : {old: ocluster, new: Array.from(ncluster)};
  }

  serializeNC() {
    return Array.from(this.ncluster);
  }

  replace(cfg) {
    if (isArray(cfg)) {
      this.ocluster = parsePeers(cfg);
      this.ncluster = new Map();
      this.majority = majorityOf(this.ocluster.size);
      this.ncMajority = 0;
    }
    else if (cfg === null || 'object' !== typeof cfg || !isArray(cfg.old) || !isArray(cfg.new)) {
      throw new TypeError('ClusterConfiguration: argument parsing error');
    }
    else {
      this.ocluster = parsePeers(cfg.old);
      this.ncluster = parsePeers(cfg.new, this.ocluster);
      this.majority = majorityOf(this.ocluster.size);
      this.ncMajority = majorityOf(this.ncluster.size);
    }
    this.updatePeers();
  }

  updatePeers() {
    var peers = this.peers;
    peers.clear();
    mergeMaps(peers, this.ocluster, this.ncluster);
    this.peersAry = Array.from(peers);
    peers.delete(this.peerId);
  }

  join(cfg) {
    if (this.ncMajority !== 0) return false;
    this.ncluster = parsePeers(cfg);
    this.ncMajority = majorityOf(this.ncluster.size);
    this.updatePeers();
    return true;
  }

  votingStart() {
    this.voted.clear();
    this.ncVoted.clear();
    this.votes = 0;
    this.ncVotes = 0;
  }

  vote(peerId, voteGranted) {
    if (this.ocluster.has(peerId)) {
      this.voted.add(peerId);
      if (voteGranted) ++this.votes;
    }
    if (this.ncluster.has(peerId)) {
      this.ncVoted.add(peerId);
      if (voteGranted) ++this.ncVotes;
    }
  }

  majorityHasVoted() {
    return this.voted.size >= this.majority && this.ncVoted.size >= this.ncMajority;
  }

  hasWonVoting() {
    return this.votes >= this.majority && this.ncVotes >= this.ncMajority;
  }

  majorityHasLogIndex(logIndex, matchIndex) {
    var majority = this.majority
      , ncMajority = this.ncMajority
      , ocluster = this.ocluster
      , ncluster = this.ncluster
      , peerId = this.peerId;

    /* assuming me has logIndex in log */
    if (ocluster.has(peerId)) --majority;
    if (ncluster.has(peerId)) --ncMajority;
    if (majority <= 0 && ncMajority <= 0) return true;

    for(peerId of this.peers.keys()) {
      if (matchIndex.get(peerId) >= logIndex) {
        if (ocluster.has(peerId)) --majority;
        if (ncluster.has(peerId)) --ncMajority;
        if (majority <= 0 && ncMajority <= 0) return true;
      }
    }

    return false;
  }

  createOtherPeersMap(factory) {
    return this.updateOtherPeersMap(new Map(), factory);
  }

  resetOtherPeersMap(map, value) {
    map.clear();

    for(var peerId of this.peers.keys()) map.set(peerId, value);

    return map;
  }

  updateOtherPeersMap(map, factory, destructor) {
    var peers = this.peers
      , peerId;

    for(peerId of map.keys()) {
      if (!peers.has(peerId)) {
        destructor && destructor(map.get(peerId), peerId);
        map.delete(peerId);
      }
    }

    for(peerId of peers.keys()) {
      if (!map.has(peerId)) {
        map.set(peerId, factory(peers.get(peerId), peerId));
      }
    }

    return map;
  }

}

ClusterConfiguration.prototype.toJSON = ClusterConfiguration.prototype.serialize;

exports = module.exports = ClusterConfiguration;
