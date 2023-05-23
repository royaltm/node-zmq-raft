/*
 *  Copyright (c) 2017-2023 Rafał Michalski <royal@yeondir.com>
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
    t.equal(cc.peerId, '1');
    t.equal(cc.isMember('1'), true);
    t.equal(cc.isMember('2'), false);
    t.equal(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.equal(cc.getUrl('2'), undefined);
    t.equal(cc.isTransitional, false);
    t.equal(cc.isSoleMaster, true);
    t.equal(cc.majority, 1);
    t.equal(cc.ncMajority, 0);
    t.same(Array.from(cc.peers), []);
    t.same(cc.configAry, [['1', 'tcp://127.0.0.1:8047']]);
    t.same(cc.serialize(), [['1', 'tcp://127.0.0.1:8047']]);
    t.same(cc.serializeNC(), []);

    t.throws(() => cc.join([['1', 'tcp://127.0.0.1:8047']]), new Error("no change in cluster membership"));
    t.throws(() => cc.join([['1', 'tcp://127.0.0.1:8147']]), new Error("new peers must be consistent with current configuration"));
    t.throws(() => cc.join([['2', 'tcp://127.0.0.1:8047']]), new Error("new peers must be consistent with current configuration"));

    t.same(cc.join([['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]),
      {old: [['1', 'tcp://127.0.0.1:8047']], new: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]});
    cc.replace(cc.join([['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]));

    t.equal(cc.isMember('1'), true);
    t.equal(cc.isMember('2'), true);
    t.equal(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.equal(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.equal(cc.isTransitional, true);
    t.equal(cc.isSoleMaster, false);
    t.equal(cc.majority, 1);
    t.equal(cc.ncMajority, 2);
    t.same(Array.from(cc.peers), [['2', 'tcp://127.0.0.1:8147']]);
    t.same(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.same(cc.serialize(), {old: [['1', 'tcp://127.0.0.1:8047']], new: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]});
    t.same(cc.serializeNC(), [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);

    t.equal(cc.join([['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]), null);

    cc.replace(cc.serializeNC());
    t.throws(() => cc.join([['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]), new Error("no change in cluster membership"));
    t.same(cc.join([['2', 'tcp://127.0.0.1:8147'], ['1', 'tcp://127.0.0.1:8047']]),
          {old: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']],
           new: [['2', 'tcp://127.0.0.1:8147'], ['1', 'tcp://127.0.0.1:8047']]});
    t.equal(cc.isMember('1'), true);
    t.equal(cc.isMember('2'), true);
    t.equal(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.equal(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.equal(cc.isTransitional, false);
    t.equal(cc.isSoleMaster, false);
    t.equal(cc.majority, 2);
    t.equal(cc.ncMajority, 0);
    t.same(Array.from(cc.peers), [['2', 'tcp://127.0.0.1:8147']]);
    t.same(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.same(cc.serialize(), [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.same(cc.serializeNC(), []);
    t.end();
  });

  suite.test('should create transitional cluster configuration', t => {
    var cc = new ClusterConfiguration('2', {old: [['1', 'tcp://127.0.0.1:8047']], new: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]});
    t.equal(cc.peerId, '2');
    t.type(cc, ClusterConfiguration);
    t.equal(cc.isMember('1'), true);
    t.equal(cc.isMember('2'), true);
    t.equal(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.equal(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.equal(cc.isTransitional, true);
    t.equal(cc.isSoleMaster, false);
    t.equal(cc.majority, 1);
    t.equal(cc.ncMajority, 2);
    t.same(Array.from(cc.peers), [['1', 'tcp://127.0.0.1:8047']]);
    t.same(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.same(cc.serialize(), {old: [['1', 'tcp://127.0.0.1:8047']], new: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]});
    t.same(cc.serializeNC(), [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);

    t.equal(cc.join([['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]), null);

    cc.replace(cc.serializeNC());
    t.equal(cc.isMember('1'), true);
    t.equal(cc.isMember('2'), true);
    t.equal(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.equal(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.equal(cc.isTransitional, false);
    t.equal(cc.isSoleMaster, false);
    t.equal(cc.majority, 2);
    t.equal(cc.ncMajority, 0);
    t.same(Array.from(cc.peers), [['1', 'tcp://127.0.0.1:8047']]);
    t.same(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.same(cc.serialize(), [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.same(cc.serializeNC(), []);

    t.same(cc.join([['1', 'tcp://127.0.0.1:8047']]),
        {old: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']], new: [['1', 'tcp://127.0.0.1:8047']]});
    cc.replace(cc.join([['1', 'tcp://127.0.0.1:8047']]));
    t.equal(cc.peerId, '2');
    t.type(cc, ClusterConfiguration);
    t.equal(cc.isMember('1'), true);
    t.equal(cc.isMember('2'), true);
    t.equal(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.equal(cc.getUrl('2'), 'tcp://127.0.0.1:8147');
    t.equal(cc.isTransitional, true);
    t.equal(cc.isSoleMaster, false);
    t.equal(cc.majority, 2);
    t.equal(cc.ncMajority, 1);
    t.same(Array.from(cc.peers), [['1', 'tcp://127.0.0.1:8047']]);
    t.same(cc.configAry, [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']]);
    t.same(cc.serialize(), {old: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147']], new: [['1', 'tcp://127.0.0.1:8047']]});
    t.same(cc.serializeNC(), [['1', 'tcp://127.0.0.1:8047']]);

    cc.replace(cc.serializeNC());
    t.equal(cc.isMember('1'), true);
    t.equal(cc.isMember('2'), false);
    t.equal(cc.getUrl('1'), 'tcp://127.0.0.1:8047');
    t.equal(cc.getUrl('2'), undefined);
    t.equal(cc.isTransitional, false);
    t.equal(cc.isSoleMaster, false);
    t.equal(cc.majority, 1);
    t.equal(cc.ncMajority, 0);
    t.same(Array.from(cc.peers), [['1', 'tcp://127.0.0.1:8047']]);
    t.same(cc.configAry, [['1', 'tcp://127.0.0.1:8047']]);
    t.same(cc.serialize(), [['1', 'tcp://127.0.0.1:8047']]);
    t.same(cc.serializeNC(), []);

    t.end();
  });

  suite.test('should help with elections', t => {
    var cc = new ClusterConfiguration('1', [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147'], ['3', 'tcp://127.0.0.1:8247']]);
    cc.votingStart();
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('3', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);

    cc.votingStart();
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('3', true);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), true);

    cc.votingStart();
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('2', true);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), true);
    cc.vote('3', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), true);
    t.end();
  });

  suite.test('should help with transitional elections', t => {
    var cc = new ClusterConfiguration('1', {
      old: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147'], ['3', 'tcp://127.0.0.1:8247']],
      new: [['1', 'tcp://127.0.0.1:8047'], ['4', 'tcp://127.0.0.1:8347'], ['5', 'tcp://127.0.0.1:8447']]
    });
    cc.votingStart();
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('3', false);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('4', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('5', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);

    cc.votingStart();
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('3', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('4', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('5', true);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), true);

    cc.votingStart();
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('2', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('3', false);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('4', true);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), true);
    cc.vote('5', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), true);

    cc.votingStart();
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('2', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('3', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('4', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('5', false);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);

    cc.votingStart();
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('1', true);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('2', false);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('3', false);
    t.equal(cc.majorityHasVoted(), false);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('4', true);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);
    cc.vote('5', true);
    t.equal(cc.majorityHasVoted(), true);
    t.equal(cc.hasWonVoting(), false);

    t.end();
  });

  suite.test('should help with log commits', t => {
    var cc = new ClusterConfiguration('1', [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147'], ['3', 'tcp://127.0.0.1:8247']]);

    var matchIndex = cc.createOtherPeersMap(() => 42);

    t.same(Array.from(matchIndex), [['2', 42], ['3', 42]]);

    t.equal(cc.majorityHasLogIndex(43, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(42, matchIndex), true);
    t.equal(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('2', 41);
    t.equal(cc.majorityHasLogIndex(43, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(42, matchIndex), true);
    t.equal(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('3', 41);
    t.equal(cc.majorityHasLogIndex(43, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(42, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(41, matchIndex), true);

    t.end();
  });

  suite.test('should help with transitional log commits', t => {
    var cc = new ClusterConfiguration('1', {
      old: [['1', 'tcp://127.0.0.1:8047'], ['2', 'tcp://127.0.0.1:8147'], ['3', 'tcp://127.0.0.1:8247']],
      new: [['1', 'tcp://127.0.0.1:8047'], ['4', 'tcp://127.0.0.1:8347'], ['5', 'tcp://127.0.0.1:8447']]
    });
    var matchIndex = cc.createOtherPeersMap(() => 42);

    t.same(Array.from(matchIndex), [['2', 42], ['3', 42], ['4', 42], ['5', 42]]);

    t.equal(cc.majorityHasLogIndex(43, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(42, matchIndex), true);
    t.equal(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('2', 41);
    t.equal(cc.majorityHasLogIndex(43, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(42, matchIndex), true);
    t.equal(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('3', 41);
    t.equal(cc.majorityHasLogIndex(43, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(42, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('3', 42);
    matchIndex.set('4', 41);
    t.equal(cc.majorityHasLogIndex(43, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(42, matchIndex), true);
    t.equal(cc.majorityHasLogIndex(41, matchIndex), true);
    matchIndex.set('5', 41);
    t.equal(cc.majorityHasLogIndex(43, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(42, matchIndex), false);
    t.equal(cc.majorityHasLogIndex(41, matchIndex), true);

    t.end();
  });

  suite.test('should update other peers map', t => {
    t.plan(15);
    var cc = new ClusterConfiguration('1', [['1', 'tcp://127.0.0.1:8047'],['2', 'tcp://127.0.0.1:8147'],['3', 'tcp://127.0.0.1:8247']]);
    t.type(cc, ClusterConfiguration);
    var map = new Map([['1', 1],['2', 2],['4', 4]]);
    t.equal(cc.updateOtherPeersMap(map), map);
    t.equal(map.size, 1);
    t.same(Array.from(map), [['2', 2]]);
    map.set('5', 5);
    t.equal(cc.updateOtherPeersMap(map, (url, id) => (id + '_' + url)), map);
    t.equal(map.size, 2);
    t.same(Array.from(map), [['2', 2],['3', '3_tcp://127.0.0.1:8247']]);
    map.delete('2');
    map.set('42', -42);
    t.equal(cc.updateOtherPeersMap(map, (url, id) => (id + '_' + url), (v, id) => {
        t.equal(v, -42);
        t.equal(id, '42');
    }), map);
    t.equal(map.size, 2);
    t.same(Array.from(map), [['3', '3_tcp://127.0.0.1:8247'],['2', '2_tcp://127.0.0.1:8147']]);
    map.clear();
    t.equal(cc.updateOtherPeersMap(map, (url, id) => (url + '#' + id), (v, id) => {
        t.fail('nothing to destroy');
    }), map);
    t.equal(map.size, 2);
    t.same(Array.from(map), [['2', 'tcp://127.0.0.1:8147#2'],['3', 'tcp://127.0.0.1:8247#3']]);
  });

  suite.test('should reset other peers map', t => {
    var cc = new ClusterConfiguration('1', [['1', 'tcp://127.0.0.1:8047'],['2', 'tcp://127.0.0.1:8147'],['3', 'tcp://127.0.0.1:8247'],['4', 'tcp://127.0.0.1:8347']]);
    t.type(cc, ClusterConfiguration);
    var map = new Map([['4', 4],['2', 2],['1', 1]]);
    t.equal(cc.resetOtherPeersMap(map, 42), map);
    t.equal(map.size, 3);
    t.same(Array.from(map), [['2', 2],['3', 42],['4', 4]]);
    map.delete('2');
    cc.replace(cc.join([['5', 'tcp://127.0.0.1:8447']]));
    t.equal(cc.resetOtherPeersMap(map, 44), map);
    t.equal(map.size, 4);
    t.same(Array.from(map), [['2', 44],['3', 42],['4', 4],['5', 44]]);

    t.end();
  });

  suite.end();
});
