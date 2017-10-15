/* 
 *  Copyright (c) 2017 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;

const raft = require('../..');
const { ClusterConfiguration } = raft.common;


test('ClusterConfiguration', suite => {

  suite.test('should create cluster configuration', t => {
    var cc = new ClusterConfiguration('1', [['1', 'tcp://127.0.0.1:8047']]);
    t.type(cc, ClusterConfiguration);
    t.strictEquals(cc.peerId, '1');
    t.strictEquals(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.strictEquals(cc.getUrl('2'), undefined);
    t.strictEquals(cc.isTransitional, false);
    t.strictEquals(cc.isSoleMaster, true);
    t.strictEquals(cc.majority, 1);
    t.strictEquals(cc.ncMajority, 0);
    t.deepEquals(Array.from(cc.peers), []);
    t.deepEquals(cc.configAry, [['1', 'tcp://127.0.0.1:8047']]);
    t.deepEquals(cc.serialize(), [['1', 'tcp://127.0.0.1:8047']]);
    t.deepEquals(cc.serializeNC(), []);

    t.strictEquals(cc.join([['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]), true);
    t.strictEquals(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.strictEquals(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.strictEquals(cc.isTransitional, true);
    t.strictEquals(cc.isSoleMaster, false);
    t.strictEquals(cc.majority, 1);
    t.strictEquals(cc.ncMajority, 2);
    t.deepEquals(Array.from(cc.peers), [['2', 'tcp://127.0.0.1:8147']]);
    t.deepEquals(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.deepEquals(cc.serialize(), {old: [['1', 'tcp://127.0.0.1:8047']], new: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]});
    t.deepEquals(cc.serializeNC(), [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);

    t.strictEquals(cc.join([['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]), false);

    cc.replace(cc.serializeNC());
    t.strictEquals(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.strictEquals(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.strictEquals(cc.isTransitional, false);
    t.strictEquals(cc.isSoleMaster, false);
    t.strictEquals(cc.majority, 2);
    t.strictEquals(cc.ncMajority, 0);
    t.deepEquals(Array.from(cc.peers), [['2', 'tcp://127.0.0.1:8147']]);
    t.deepEquals(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.deepEquals(cc.serialize(), [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.deepEquals(cc.serializeNC(), []);
    t.end();
  });

  suite.test('should create transitional cluster configuration', t => {
    var cc = new ClusterConfiguration('2', {old: [['1', 'tcp://127.0.0.1:8047']], new: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]});
    t.strictEquals(cc.peerId, '2');
    t.type(cc, ClusterConfiguration);
    t.strictEquals(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.strictEquals(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.strictEquals(cc.isTransitional, true);
    t.strictEquals(cc.isSoleMaster, false);
    t.strictEquals(cc.majority, 1);
    t.strictEquals(cc.ncMajority, 2);
    t.deepEquals(Array.from(cc.peers), [['1', 'tcp://127.0.0.1:8047']]);
    t.deepEquals(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.deepEquals(cc.serialize(), {old: [['1', 'tcp://127.0.0.1:8047']], new: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]});
    t.deepEquals(cc.serializeNC(), [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);

    t.strictEquals(cc.join([['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]), false);

    cc.replace(cc.serializeNC());
    t.strictEquals(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.strictEquals(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.strictEquals(cc.isTransitional, false);
    t.strictEquals(cc.isSoleMaster, false);
    t.strictEquals(cc.majority, 2);
    t.strictEquals(cc.ncMajority, 0);
    t.deepEquals(Array.from(cc.peers), [['1', 'tcp://127.0.0.1:8047']]);
    t.deepEquals(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.deepEquals(cc.serialize(), [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.deepEquals(cc.serializeNC(), []);

    t.strictEquals(cc.join([['1', 'tcp://127.0.0.1:8047']]), true);
    t.strictEquals(cc.peerId, '2');
    t.type(cc, ClusterConfiguration);
    t.strictEquals(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.strictEquals(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.strictEquals(cc.isTransitional, true);
    t.strictEquals(cc.isSoleMaster, false);
    t.strictEquals(cc.majority, 2);
    t.strictEquals(cc.ncMajority, 1);
    t.deepEquals(Array.from(cc.peers), [['1', 'tcp://127.0.0.1:8047']]);
    t.deepEquals(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.deepEquals(cc.serialize(), {old: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']], new: [['1', 'tcp://127.0.0.1:8047']]});
    t.deepEquals(cc.serializeNC(), [['1', 'tcp://127.0.0.1:8047']]);

    cc.replace(cc.serializeNC());
    t.strictEquals(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.strictEquals(cc.getUrl('2'), undefined);
    t.strictEquals(cc.isTransitional, false);
    t.strictEquals(cc.isSoleMaster, true);
    t.strictEquals(cc.majority, 1);
    t.strictEquals(cc.ncMajority, 0);
    t.deepEquals(Array.from(cc.peers), [['1', 'tcp://127.0.0.1:8047']]);
    t.deepEquals(cc.configAry, [['1', 'tcp://127.0.0.1:8047']]);
    t.deepEquals(cc.serialize(), [['1', 'tcp://127.0.0.1:8047']]);
    t.deepEquals(cc.serializeNC(), []);

    t.end();
  });

  suite.test('should help with elections', t => {
    var cc = new ClusterConfiguration('1', [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147'], ['3', 'tcp://127.0.0.1:8247']]);
    cc.votingStart();
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('3', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);

    cc.votingStart();
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('3', true);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), true);

    cc.votingStart();
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('2', true);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), true);
    cc.vote('3', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), true);
    t.end();
  });

  suite.test('should help with transitional elections', t => {
    var cc = new ClusterConfiguration('1', {
      old: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147'], ['3', 'tcp://127.0.0.1:8247']],
      new: [['1', 'tcp://127.0.0.1:8047'], ['4', 'tcp://127.0.0.1:8347'], ['5', 'tcp://127.0.0.1:8447']]
    });
    cc.votingStart();
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('3', false);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('4', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('5', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);

    cc.votingStart();
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('3', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('4', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('5', true);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), true);

    cc.votingStart();
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('2', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('3', false);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('4', true);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), true);
    cc.vote('5', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), true);

    cc.votingStart();
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('2', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('3', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('4', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('5', false);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);

    cc.votingStart();
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('3', false);
    t.strictEquals(cc.majorityHasVoted(), false);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('4', true);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);
    cc.vote('5', true);
    t.strictEquals(cc.majorityHasVoted(), true);
    t.strictEquals(cc.hasWonVoting(), false);

    t.end();
  });

  suite.test('should help with log commits', t => {
    var cc = new ClusterConfiguration('1', [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147'], ['3', 'tcp://127.0.0.1:8247']]);

    var matchIndex = cc.createOtherPeersMap(() => 42);

    t.deepEquals(Array.from(matchIndex), [['2', 42], ['3', 42]]);

    t.strictEquals(cc.majorityHasLogIndex(43, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(42, matchIndex), true);
    t.strictEquals(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('2', 41);
    t.strictEquals(cc.majorityHasLogIndex(43, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(42, matchIndex), true);
    t.strictEquals(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('3', 41);
    t.strictEquals(cc.majorityHasLogIndex(43, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(42, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(41, matchIndex), true);

    t.end();
  });

  suite.test('should help with transitional log commits', t => {
    var cc = new ClusterConfiguration('1', {
      old: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147'], ['3', 'tcp://127.0.0.1:8247']],
      new: [['1', 'tcp://127.0.0.1:8047'], ['4', 'tcp://127.0.0.1:8347'], ['5', 'tcp://127.0.0.1:8447']]
    });
    var matchIndex = cc.createOtherPeersMap(() => 42);

    t.deepEquals(Array.from(matchIndex), [['2', 42], ['3', 42], ['4', 42], ['5', 42]]);

    t.strictEquals(cc.majorityHasLogIndex(43, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(42, matchIndex), true);
    t.strictEquals(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('2', 41);
    t.strictEquals(cc.majorityHasLogIndex(43, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(42, matchIndex), true);
    t.strictEquals(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('3', 41);
    t.strictEquals(cc.majorityHasLogIndex(43, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(42, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('3', 42);
    matchIndex.set('4', 41);
    t.strictEquals(cc.majorityHasLogIndex(43, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(42, matchIndex), true);
    t.strictEquals(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('5', 41);
    t.strictEquals(cc.majorityHasLogIndex(43, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(42, matchIndex), false);
    t.strictEquals(cc.majorityHasLogIndex(41, matchIndex), true);

    t.end();
  });

  suite.end();
});
