#!/usr/bin/env node
/* 
 *  Copyright (c) 2016-2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

if (require.main !== module) throw new Error("zr-config.js must be run directly from node");

const path = require('path')
    , fs = require('fs')
    , url = require('url')
    , { isIP } = require('net')
    , assert = require('assert');

const program = require('commander');

const pkg = require('../package.json');

const raft = require('..');

const { client: { ZmqRaftClient }
      , utils: { id: { genIdent, isIdent }
               , helpers: { parsePeers } }
      } = raft;

const lookup = require('../lib/utils/dns_lookup').hostsToZmqUrls;

program
  .version(pkg.version)
  .usage('[options] <host[:port] ...>')
  .option('-s, --show', 'Display new configuration instead of changing it')
  .option('-a, --add <peers>', 'Add specified peers to the cluster')
  .option('-r, --replace <peers>', 'Replace the cluster with specified peers')
  .option('-d, --delete <peers>', 'Delete specified peers from the cluster')
  .option('-t, --timeout <msecs>', 'Cluster conneciton timeout', parseInt)
  .option('--ident <ident>', 'Specify your own request id (DANGEROUS)')
  // .option('-f, --file <file>', 'Replace the cluster with peers from a file in json format')
  .parse(process.argv);

if (program.args.length === 0) program.help();

if (!program.add && !program.replace && !program.delete) {
  program.show = true;
}

if (program.replace) {
  if (program.add || program.delete) {
    exitError(1, 'specify --replace is mutually exclusive with --add and --delete');
  }
}

function parsePeerUrl(peer) {
  var id;
  const {protocol, host, pathname} = url.parse(peer);
  if (pathname) {
    id = decodeURIComponent(pathname.substring(1));
    assert(id.length !== 0, 'peer id must not be empty');
  }
  peer = url.format({protocol, host, slashes:true});
  if (id === undefined) id = peer;
  return {id, url: peer};
}

function parsePeerUrls(peers, currentPeers) {
  return parsePeers(peers.split(/\s*,\s*/).map(parsePeerUrl), currentPeers);
}

function configIsSame(oldcfg, newcfg) {
  if (oldcfg.size !== newcfg.size) return false;
  return Array.from(newcfg).every(([id, url]) => {
    return oldcfg.get(id) === url;
  });
}

function getIdent() {
  var id = program.ident || genIdent();
  if (!isIdent(id)) {
    throw new TypeError("invalid ident format");
  }
  return id;
}

function objectToAry(obj) {
  return Object.keys(obj).map(key => [key, obj[key]]);
}

function exitError(status) {
  args = [].slice.call(arguments, 1);
  console.error.apply(console, args);
  process.exit(status);
}

function start() {
  return lookup(program.args)
  .then(urls => new ZmqRaftClient(urls))
  .then(client => {
    return client.requestConfig(program.timeout)
    .then(config => {
      var newcfg
        , oldcfg = parsePeers(objectToAry(config.urls));

      if (program.replace) {
        newcfg = parsePeerUrls(program.replace, oldcfg);
      }
      else {
        newcfg = new Map(oldcfg);
        if (program.add) {
          parsePeerUrls(program.add, oldcfg).forEach((url, id) => {
            if (oldcfg.has(id)) throw new Error('trying to add peer already found in the cluster: ' + id);
            newcfg.set(id, url);
          });
        }
        if (program.delete) {
          parsePeerUrls(program.delete, oldcfg).forEach((url, id) => {
            if (!oldcfg.has(id)) throw new Error('trying to remove peer not found in the cluster: ' + id);
            newcfg.delete(id);
          });
        }
      }

      if (!configIsSame(oldcfg, newcfg)) {
        const peers = Array.from(newcfg)
            , ident = getIdent();

        showConfig(newcfg, null, 'Requesting configuration change with ' + ident, oldcfg);

        if (!program.show) {
          return client.configUpdate(ident, peers)
          .then(result => {
            console.log("Cluster configuration changed at: %s", result);
          });
        }
      } else {
        showConfig(oldcfg, config.leaderId, 'Current cluster configuration (no changes)');
      }
    })
    .then(() => client.close());
  });
}

function formatPeer(id, url) {
  if (id === url) return id;
  else return url + '/' + encodeURIComponent(id);
}

function showConfig(cfg, leaderId, label, oldcfg) {
  console.log("%s:", label);
  cfg.forEach((url, id) => {
    var peer = formatPeer(id, url);
    if (oldcfg) {
      if (!oldcfg.has(id)) peer += ' (added)';
    }
    else if (id === leaderId) {
      peer += ' (leader)';
    }
    console.log("  %s", peer);
  });
  if (oldcfg) {
    oldcfg.forEach((url, id) => {
      if (!cfg.has(id)) {
        console.log("  %s (removed)", formatPeer(id, url));
      }
    });
  }
  console.log('');
}

Promise.resolve().then(start).catch(err => {
  console.error("CLUSTER CONFIGURATION ERROR");
  console.error(err.stack);
  process.exit(1);
});
