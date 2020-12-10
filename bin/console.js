#!/usr/bin/env node
"use strict";
/*
 * BIN console
 *
 * Author: Rafal Michalski (c) 2017-2020
 */

const assert = require('assert');
const os = require('os');
const dns = require('dns');
const fs = require('fs');
const path = require('path');
const util = require('util');
const crypto = require('crypto');
const { createUnzip } = require('zlib');

const defaultConfig = path.join(__dirname, '..', 'config', 'console.hjson');

const debug = require('debug')('zmq-raft:console');

const isArray = Array.isArray;

const pkg = require('../package.json');

const ben = require('ben');

const colors = require('colors/safe')
    , { cyan, green, grey, magenta, red, yellow, bgGreen } = colors;

const msgpack = require('msgpack-lite');

const lookup = require('../lib/utils/dns_lookup').hostsToZmqUrls;

const raft = require('..');

const { genIdent } = raft.utils.id;
const { ZmqRaftPeerClient
      , ZmqRaftPeerSub
      , ZmqRaftClient
      , ZmqRaftSubscriber } = raft.client;
const { LOG_ENTRY_TYPE_STATE, LOG_ENTRY_TYPE_CONFIG, LOG_ENTRY_TYPE_CHECKPOINT
      , readers: { readTypeOf, readTermOf, readDataOf } } = raft.common.LogEntry;

const { createRepl } = require('../lib/utils/repl');
const { listPeers
      , showInfo
      , argToBoolean
      , prompt
      , replError } = require('../lib/utils/repl_helpers');
const { readConfig } = require('../lib/utils/config');

const argv = process.argv.slice(2);

const motd = util.format("%s REPL, welcome!", pkg.name);

console.log(grey("-".repeat(motd.length)));
console.log(motd);
console.log("version: %s", cyan(pkg.version));
console.log(grey("-".repeat(motd.length)));

const dbs = new Map();
var currentdb;

var subs, client;

function getClient() {
  return subs ? subs.client : client;
}

readConfig(argv[0] || defaultConfig, "raft")
.then(config => createRepl().then(repl => {
  repl.on('reset', initializeContext);
  repl.on('exitsafe', () => {
    console.log('Received "exit" event from repl!');
    process.exit();
  });
  repl.defineCommand('peers', {
    help: 'Show db remote cluster config',
    action: function() {
      listPeers(getClient()).then(() => prompt(repl)).catch(error);
    }
  });
  repl.defineCommand('info', {
    help: 'Show db remote log information of a specified peer or any peer',
    action: function(id) {
      showInfo(getClient(), id).then(() => prompt(repl)).catch(error);
    }
  });
  repl.defineCommand('secret', {
    help: "Set cluster's secret part of the protocol before connecting",
    action: function(secret) {
      if (secret) repl.context.secret = secret;
      else console.log('Current secret: %j', repl.context.secret);
      prompt(repl);
    }
  });
  repl.defineCommand('cpeer', {
    help: 'Connect to a single zmq-raft peer: host[:port]',
    action: function(host) {
      if (!host) {
        if (client) {
          console.log("currently connected to:");
          client.urls.forEach(url => console.log(" - %s", grey(url)));
        }
        else {
          console.log(yellow("not connected"));
        }
        prompt(repl)
      }
      else {
        lookup(host).then(urls => {
          shutdownClients();
          const opts = Object.assign({}, config.console.client,
          {
            secret: repl.context.secret
          });
          client = new ZmqRaftPeerClient(urls[0], opts);
          repl.context.client = client;
          console.log('connecting to peer: %s', urls[0]);
        })
        .then(() => prompt(repl)).catch(error);
      }
    }
  });
  repl.defineCommand('pulse', {
    help: 'Turn on/off reporting peer-sub\'s "pulse" events: [off]',
    action: function(arg) {
      config.console.pulse = !/off|no|0/i.test(arg);
      if (client && typeof client.on === 'function') {
        client.removeAllListeners('pulse');
        if (config.console.pulse) {
          console.log("listening for pulses on: %s", green(client.url || '(none)'));
          client.on('pulse', pulseHandler);
        }
      }
      else {
        console.log(yellow("1st subscribe with: .subp ADDRESS[:PORT]"));
      }
      prompt(repl);
    }
  });
  repl.defineCommand('subp', {
    help: 'Subscribe to a single broadcast state peer: [host[:port]|lastIndex]',
    action: function(host) {
      if (!host || /^\d+$/.test(host)) {
        if (client && client.foreverEntriesStream) {
          if (subs && !subs.destroyed) {
            subs.destroy();
          }
          let lastIndex = parseInt(host);
          if (!Number.isFinite(lastIndex)) {
            lastIndex = config.console.subscriber.lastIndex;
          }
          subs = client.foreverEntriesStream(lastIndex);
          repl.context.subs = subs;
          subs.on('end', () => {
            console.log(cyan("forever stream ends, peer is not a LEADER"))
          });
          subs.on('close', () => {
            console.log(red("forever stream closed"));
          });
          subs.on('data', showSubStreamEntry);
          subs.on('error', error);
        }
        else {
          console.log(yellow("subscribe to a peer first"));
        }
        prompt(repl);
      }
      else {
        lookup(host).then(urls => {
          shutdownClients();
          const opts = Object.assign({},
            config.console.client,
            config.console.subscriber,
          {
            secret: repl.context.secret,
          });
          client = new ZmqRaftPeerSub(urls, opts);
          repl.context.client = client;
          console.log('subscribing to peer: %s', urls[0]);
          client.on('error', error);
          client.on('pub', url => console.log("PUB url: %s", green(url)));
          if (config.console.pulse) client.on('pulse', pulseHandler);
          client.on('timeout', () => console.log(red("(X) BSM timeout!")));
          client.on('close', () => console.log(grey("peer subscriber closed")));
        })
        .then(() => prompt(repl)).catch(error);
      }
    }
  });
  repl.defineCommand('connect', {
    help: 'Connect a client to zmq-raft servers: host[:port] [host...]',
    action: function(hosts) {
      lookup(hosts.split(/\s+/)).then(urls => {
        shutdownClients();
        const opts = Object.assign({ heartbeat: 5000 },
          config.console.client,
        {
          secret: repl.context.secret
        });
        client = new ZmqRaftClient(urls, opts);
        repl.context.client = client;
        console.log('connecting to: %s', urls.join(', '));
      })
      .then(() => prompt(repl)).catch(error);
    }
  });
  repl.defineCommand('subscribe', {
    help: 'Subscribe to zmq-raft broadcast state servers: host[:port] [host...]',
    action: function(hosts) {
      lookup(hosts.split(/\s+/)).then(urls => {
        shutdownClients();
        const opts = Object.assign({},
          config.console.client,
          config.console.subscriber,
        {
          secret: repl.context.secret,
          heartbeat: 0
        });
        subs = new ZmqRaftSubscriber(urls, opts);
        repl.context.client = subs.client;
        repl.context.subs = subs;
        console.log('subscribing to: %s', urls.join(', '));
        subs.on('error', error);
        subs.on('data', showSubStreamEntry);
      })
      .then(() => prompt(repl)).catch(error);
    }
  });
  repl.defineCommand('close', {
    help: 'Close current connection or subscription',
    action: function(hosts) {
      shutdownClients();
      console.log('closed');
      prompt(repl);
    }
  });
  repl.defineCommand('start', {
    help: 'Flood server with updates: [data|+BURST_COUNT]',
    action: function(data) {
      var burstcount = 1;
      const flood = repl.context.flood;
      if (data) {
        let mt = data.match(/^\+(\d+)$/);
        if (mt) {
          burstcount = mt[1]|0;
          if (burstcount < 1 || burstcount > 100) {
            console.log(yellow("BURST_COUNT should be between 1 and 100"))
            return prompt(repl);
          }
        }
        else flood.data = data;
      }
      if (!flood.flooding) {
        if (subs && subs.write) {
          if (burstcount !== 1) {
            console.log(yellow("to set BURST_COUNT for subscriber flood, set:\n") +
                               "  config.subscriber.duplex = {writableHighWaterMark: %s}\n" +
                          grey("before .subscribe ..."), burstcount);
            return prompt(repl);
          }
          startFloodingStream(repl, subs);
        }
        else if (client) startFloodingClient(repl, client, burstcount);
        else console.log(yellow('not connected'));
      }
      prompt(repl);
    }
  });
  repl.defineCommand('decompress', {
    help: 'Snapshot data is compressed: yes|no',
    action: function(arg) {
      if (arg) {
        repl.context.entries.decompress = argToBoolean(arg);
      }
      console.log('decompress: %j', repl.context.entries.decompress);
      prompt(repl);
    }
  });
  repl.defineCommand('data', {
    help: 'Set flood and verify data: <data>',
    action: function(data) {
      if (data) repl.context.flood.data = data;
      console.log('Current data: %j', repl.context.flood.data);
      prompt(repl);
    }
  });
  repl.defineCommand('stop', {
    help: 'Stop flooding server with updates',
    action: function() {
      repl.context.flood.flooding = false;
      prompt(repl);
    }
  });
  repl.defineCommand('iter', {
    help: 'Reset flooding iteration',
    action: function(iteration) {
      if (iteration !== '') {
        iteration = +iteration || 0;
        repl.context.flood.flooding = false;
        repl.context.flood.iteration = iteration;
      }
      console.log('Last iteration: %s', repl.context.flood.iteration);
      prompt(repl);
    }
  });
  repl.defineCommand('decode', {
    help: 'Set decoding of read entries: mp|hex|no',
    action: function(args) {
      switch(args) {
        case 'mp':
        case 'msgpack':
          repl.context.entries.decode = buf => msgpack.decode(buf);
          console.log('entries decoding: %s', magenta('msgpack'));
          break;
        case 'hex':
          repl.context.entries.decode = buf => buf.toString('hex');
          console.log('entries decoding: %s', magenta('hex'));
          break;
        default:
          repl.context.entries.decode = buf => buf.length;
          console.log('entries decoding: %s', magenta('none'));
      }
      prompt(repl);
    }
  });
  repl.defineCommand('read', {
    help: 'Read log entries: [lastIndex=0] [count]',
    action: function(args) {
      var client = repl.context.client;
      if (!client) {
        console.log(yellow('not connected'));
        return prompt(repl);
      }
      var [index, count] = args.split(/\s+/);
      const decode = repl.context.entries.decode;
      index = +index || 0;
      count |= 0;
      console.log('Reading last log index: %s count: %s', index, count > 0 ? count : '-');
      client.requestEntries(index, count,
        (status, entries, lastIndex, byteOffset, snapshotSize, isLastChunk, snapshotTerm) => {
          if (isArray(entries)) {
            const firstIndex = lastIndex - entries.length + 1;
            entries.forEach((entry, index) => {
              switch(readTypeOf(entry)) {
              case LOG_ENTRY_TYPE_STATE:
                console.log('state logIndex: %s logTerm: %s data: %j', firstIndex + index, readTermOf(entry), decode(readDataOf(entry)));
                break;
              case LOG_ENTRY_TYPE_CONFIG:
                console.log('config logIndex: %s logTerm: %s', firstIndex + index, readTermOf(entry));
                break;
              case LOG_ENTRY_TYPE_CHECKPOINT:
                console.log('checkpoint logIndex: %s logTerm: %s', firstIndex + index, readTermOf(entry));
                break;
              }
            });
          }
          else {
            console.log('snapshot chunk offset: %s of %s (%s bytes)', byteOffset, snapshotSize, entries.length);
            if (isLastChunk) {
              console.log('snapshot done, logIndex %s logTerm %s', lastIndex, snapshotTerm);
            }
          }
        })
      .then(status => {
        console.log('read server log done: %s', status);
        prompt(repl);
      })
      .catch(error);
    }
  });
  repl.defineCommand('verify', {
    help: 'Verify log entries: [startLogIndex=1] [startIteration=1] [count]',
    action: function(args) {
      var client = repl.context.client;
      if (!client) {
        console.log(yellow('not connected'));
        return prompt(repl);
      }
      var [startLogIndex, iteration, count] = args.split(/\s+/);
      startLogIndex = +startLogIndex || 1; if (startLogIndex <= 0) startLogIndex = 1;
      iteration = +iteration || 1; if (iteration <= 0) iteration = 1;
      const floodData = repl.context.flood.data;
      count |= 0;
      console.warn('Verifying start log index: %s iteration: %s data: %j', startLogIndex, iteration, floodData);
      const dec = msgpack.createDecodeStream();
      var decompress = !!repl.context.entries.decompress;
      dec.on('data', item => {
        assert.notStrictEqual(item, null);
        assert.strictEqual(typeof item, 'object');
        if (item.data === floodData) {
          assert.strictEqual(item.iteration, iteration++);
          console.warn('verified item: %s create time: %s', item.iteration, item.time.toISOString());
        }
      })
      .on('error', err => replError(repl, err))
      .on('finish', () => {
        console.log('verify server log done');
        prompt(repl);
      });

      const entriesStream = client.requestEntriesStream(startLogIndex - 1, count)
      .on('error', err => replError(repl, err))
      .on('data', chunk => {
        if (chunk.isLogEntry) {
          assert.strictEqual(startLogIndex, chunk.logIndex);
          ++startLogIndex;
          if (chunk.isStateEntry) dec.write(chunk.readEntryData());
        }
        else {
          assert(chunk.logIndex >= startLogIndex, 'snapshot logIndex < startLogIndex');
          if (decompress !== false) {
            if (decompress === true) {
              debug('creating unzip for snapshot decompression')
              decompress = createUnzip()
              .on('error', err => replError(repl, err))
              .on('end', () => {
                entriesStream.resume();
                decompress = true;
              });
              decompress.pipe(dec, {end: false});
            }
            if (chunk.isLastChunk) {
              entriesStream.pause();
              decompress.end(chunk);
            } else decompress.write(chunk);
          }
          else {
            dec.write(chunk);
          }
          if (chunk.isLastChunk) startLogIndex = chunk.logIndex + 1;
        }
      })
      .on('end', () => {
        if (typeof decompress !== 'boolean') {
          decompress.on('end', () => dec.end());
        } else dec.end();
      });
    }
  });

  const defaultWriter = repl.writer;

  repl.writer = function(output) {
    if (output !== null && 'object' === typeof output && 'function' === typeof output.then) {
      output.then(result => {
        console.log('\n' + green('Promise result:') + ' %s', defaultWriter(result));
        prompt(repl);
        repl.context.res = result;
        return result;
      });
    }
    return defaultWriter.apply(repl, arguments);
  };

  function shutdownClients() {
    if (subs && subs.close) subs.close();
    if (client && client.close) client.close();
    repl.context.subs = subs = undefined;
    repl.context.client = client = undefined;
    repl.context.flood.flooding = false;
  }

  function pulseHandler(ix, term, entries) {
    console.log(red("(( ))") + grey(" index: %s, term: %s, entries: %s"), ix, term, entries.length);
  }

  process.on('unhandledRejection', (error, promise) => {
    console.log('\n' + red('Promise rejected with: ') + '%s', error);
    if (error.stack) console.warn(error.stack);
  });

  initializeContext(repl.context);

  function initializeContext(context) {

    Object.assign(context, {
      colors
    , ri: repl
    , mp: msgpack
    , raft
    , ben
    , pkg
    , lookup
    , secret: config.secret
    , entries: {decode: buf => buf.length, logIndex: 0, decompress: true}
    , genId: genIdent
    , flood: {iteration: 0, flooding: false, data: crypto.randomBytes(6).toString('base64')}
    , config: config.console
    });
  }

  function error(err) {
    return replError(repl, err);
  }

  function showSubStreamEntry(entry) {
    var logIndex = repl.context.entries.logIndex = entry.logIndex
      , decode = repl.context.entries.decode;
    if (entry.isLogEntry) {
      switch(entry.entryType) {
      case LOG_ENTRY_TYPE_STATE:
        console.log('subscriber index: %s term: %s data: %j', logIndex, entry.readEntryTerm(), decode(entry.readEntryData()));
        break;
      case LOG_ENTRY_TYPE_CONFIG:
        console.log('subscriber index: %s term: %s config', logIndex, entry.readEntryTerm());
        break;
      case LOG_ENTRY_TYPE_CHECKPOINT:
        console.log('subscriber index: %s term: %s checkpoint', logIndex, entry.readEntryTerm());
        break;
      }
    } else {
      console.log('snapshot chunk offset: %s of %s (%s bytes)', entry.snapshotByteOffset, entry.snapshotTotalLength, entry.length);
      if (entry.isLastChunk) {
        console.log('snapshot done, logIndex %s logTerm %s', entry.logIndex, entry.logTerm);
      }
    }
  }
})).catch(err => console.warn(err.stack));

function floodingNextFactory(enc, flood) {
  return function(previous) {
    if (flood.flooding) {
      const iteration = ++flood.iteration;
      console.log('updating: %s', iteration);
      enc.encode({iteration, data: flood.data, time: new Date()});
    }
    else {
      console.log(cyan("flood over:") + " %s", previous);
    }
  }
}

function startFloodingClient(repl, client, burstcount) {
  const lastId = Buffer.alloc(12);
  const flood = repl.context.flood;
  const entries = repl.context.entries;
  const enc = new msgpack.Encoder();
  const next = floodingNextFactory(enc, flood);

  enc.on('data', buf => {
    const id = genIdent(lastId, 0);
    const iteration = flood.iteration;
    client.requestUpdate(id, buf)
    .then(index => {
      if (index > entries.logIndex) entries.logIndex = index;
      // console.log('updated log index: %s', index, buf.toString('hex'));
      next(iteration);
    })
    .catch(err => {
      if (err.isTimeout) {
        console.log(red("update response timeout for: ") + "%s", iteration);
        next(iteration);
      }
      else {
        flood.flooding = false;
        replError(repl, err)
      }
    });
  });

  flood.flooding = true;
  let count = burstcount|0;
  while (count-- > 0) next(flood.iteration);
}

function startFloodingStream(repl, subs) {
  const flood = repl.context.flood;
  const enc = new msgpack.Encoder();
  const next = floodingNextFactory(enc, flood);

  enc.on('data', buf => {
    const iteration = flood.iteration;
    buf.requestId = genIdent();
    if (subs.write(buf)) {
      next(iteration);
    }
    else {
      subs.once('drain', () => next(iteration));
    }
  });

  flood.flooding = true;
  next(flood.iteration);
}
