#!/usr/bin/env node
/*
 *  Copyright (c) 2016-2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

if (require.main !== module) throw new Error("zr-config.js must be run directly from node");

const url = require('url')
    , assert = require('assert');

const program = require('commander');

const { decode: decodeMsgPack } = require('msgpack-lite');

const pkg = require('../package.json');

const raft = require('..');

const { readConfig } = require('../lib/utils/config');

const { client: { ZmqRaftClient }
      , common: { constants: { RE_STATUS_SNAPSHOT
                             , SERVER_ELECTION_GRACE_MS }
                , LogEntry: { LOG_ENTRY_TYPE_CONFIG
                            , readers: { readTypeOf
                                       , readDataOf } } }
      , utils: { id: { genIdent, isIdent }
               , helpers: { parsePeers } }
      } = raft;

const lookup = require('../lib/utils/dns_lookup').hostsToZmqUrls;

program
  .version(pkg.version)
  .usage('[options] [host[:port] ...]')
  .description('change zmq-raft cluster peer membership')
  .option('-n, --dry-run', 'Only display what would be changed')
  .option('-c, --config <file>', 'Config file')
  .option('-k, --cluster <secret>', 'Secret cluster identity part of the protocol')
  .option('-a, --add <peers>', 'Add specified peers to the cluster')
  .option('-r, --replace <peers>', 'Replace the cluster with specified peers')
  .option('-d, --delete <peers>', 'Delete specified peers from the cluster')
  .option('-t, --timeout <msecs>', 'Cluster connection timeout', parseInt)
  .option('--ns [namespace]', 'Raft config root namespace [raft]', 'raft')
  .option('--ident <ident>', 'Specify custom request id (DANGEROUS)')
  // .option('-f, --file <file>', 'Replace the cluster with peers from a file in hjson format')
  .parse(process.argv);

const opts = program.opts()

if (!opts.add && !opts.replace && !opts.delete) {
  opts.dryRun = true;
}

if (opts.replace) {
  if (opts.add || opts.delete) {
    exitError(1, 'specify --replace is mutually exclusive with --add and --delete');
  }
}

function exitError(status) {
  var args = [].slice.call(arguments, 1);
  console.error.apply(console, args);
  process.exit(status);
}

readConfig(opts.config, opts.ns).then(config => {
  const urls = program.args;
  return Promise.resolve(urls.length === 0 ? Array.from(parsePeers(config.peers).values())
                                           : lookup(urls))
  .then(urls => {
    if (urls.length === 0) program.help();
    const secret = opts.cluster === undefined ? config.secret
                                                 : opts.cluster;
    return {urls, secret}
  });
})
.then(({urls, secret}) => new ZmqRaftClient(urls, {secret}))
.then(client => {
  return client.requestConfig(opts.timeout)
  .then(config => {
    var newcfg
      , oldcfg = parsePeers(objectToAry(config.urls));

    if (opts.replace) {
      newcfg = parsePeerUrls(opts.replace, oldcfg);
    }
    else {
      newcfg = new Map(oldcfg);
      if (opts.add) {
        parsePeerUrls(opts.add, oldcfg).forEach((url, id) => {
          if (oldcfg.has(id)) exitError(2, 'trying to add peer already found in the cluster: ' + id);
          newcfg.set(id, url);
        });
      }
      if (opts.delete) {
        parsePeerUrls(opts.delete, oldcfg).forEach((url, id) => {
          if (!oldcfg.has(id)) exitError(3, 'trying to remove peer not found in the cluster: ' + id);
          newcfg.delete(id);
        });
      }
    }

    if (!configIsSame(oldcfg, newcfg)) {
      const peers = Array.from(newcfg)
          , ident = getIdent();

      showConfig(newcfg, null, 'Requesting configuration change with ' + ident, oldcfg);

      if (!opts.dryRun) {
        return client.configUpdate(ident, peers)
        .then(index => {
          console.log("Cluster joined configuration changed at index %s.", index);
          return waitForConfig(index, client)
        })
        .then(({config, configIndex}) => {
          showConfig(parsePeers(config), client.leaderId, 'Cluster final configuration changed at index ' + configIndex);
        });
      }
    } else {
      showConfig(oldcfg, config.leaderId, 'Current cluster configuration (no changes)');
    }
  })
  .then(() => client.close());
})
.catch(err => {
  console.error("CLUSTER CONFIGURATION ERROR");
  console.error(err.stack);
  process.exit(1);
});

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
  var id = opts.ident || genIdent();
  if (!isIdent(id)) {
    exitError(3, "invalid ident format");
  }
  return id;
}

function objectToAry(obj) {
  return Object.keys(obj).map(key => [key, obj[key]]);
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

function waitForConfig(index, client) {
  var config, configIndex;

  const next = () => client.delay(SERVER_ELECTION_GRACE_MS)
  .then(() => client.requestEntries(index, (status, entries, lastIndex) => {
    if (status !== RE_STATUS_SNAPSHOT) {
      const idx = entries.findIndex(entry => readTypeOf(entry) === LOG_ENTRY_TYPE_CONFIG);
      if (idx !== -1) {
        config = decodeMsgPack(readDataOf(entries[idx]));
        configIndex = lastIndex - entries.length + 1 + idx;
        return false;
      }
    }
  }))
  .then(result => result ? next() : {config, configIndex});

  return next();
}
