#!/usr/bin/env node
"use strict";
/*
 * BIN console
 *
 * Author: Rafal Michalski (c) 2017
 */

const assert = require('assert');
const os = require('os');
const dns = require('dns');
const fs = require('fs');
const path = require('path');
const util = require('util');
const crypto = require('crypto');
const { createUnzip } = require('zlib');
const debug = require('debug')('zmq-raft:console');

const isArray = Array.isArray;

const pkg = require('../package.json');

const tmpdir = path.normalize(path.join(__dirname, '..', 'tmp'));

const ben = require('ben');

const colors = require('colors/safe')
    , { cyan, green, grey, magenta, red, yellow, bgGreen } = colors;

const msgpack = require('msgpack-lite');

const lookup = require('../lib/utils/dns_lookup').hostsToZmqUrls;

const { genIdent } = require('../lib/utils/id');
const { createRepl } = require('../lib/utils/repl');
const ZmqRaftClient = require('../lib/client/zmq_raft_client');
const ZmqRaftSubscriber = require('../lib/client/zmq_raft_subscriber');
const { lpad, regexpEscape } = require('../lib/utils/helpers');
const { createRotateName } = require('../lib/utils/filerotate');
const { LOG_ENTRY_TYPE_STATE, LOG_ENTRY_TYPE_CONFIG, LOG_ENTRY_TYPE_CHECKPOINT
      , readers: { readTypeOf, readTermOf, readDataOf } } = require('../lib/common/log_entry');

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

createRepl().then(repl => {
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
  repl.defineCommand('connect', {
    help: 'Connect client to zmq-raft servers: host [host...]',
    action: function(hosts) {
      lookup(hosts.split(/\s+/)).then(urls => {
        client && client.close();
        client = new ZmqRaftClient(urls);
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
        subs && subs.close();
        client && client.close();
        subs = new ZmqRaftSubscriber(urls);
        repl.context.client = subs.client;
        repl.context.subs = subs;
        console.log('connecting to: %s', urls.join(', '));
        subs.on('error', error);
        subs.on('data', entry => {
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
        });
      })
      .then(() => prompt(repl)).catch(error);
    }
  });
  repl.defineCommand('close', {
    help: 'Subscribe to zmq-raft broadcast state servers: host[:port] [host...]',
    action: function(hosts) {
      subs && subs.close();
      client && client.close();
      repl.context.client = undefined;
      repl.context.subs = undefined;
      repl.context.flood.flooding = false;
      console.log('closed');
      prompt(repl);
    }
  });
  repl.defineCommand('start', {
    help: 'Flood server with updates: [data]',
    action: function(data) {
      const flood = repl.context.flood;
      if (data) flood.data = data;
      if (!flood.flooding) {
        if (subs) startFloodingStream(repl, subs);
        else if (client) startFloodingClient(repl, client);
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
    , ben
    , pkg
    , lookup
    , entries: {decode: buf => buf.length, logIndex: 0, decompress: true}
    , genId: genIdent
    , flood: {iteration: 0, flooding: false, data: crypto.randomBytes(6).toString('base64')}
    });
  }

  function error(err) {
    return replError(repl, err);
  }

}).catch(err => console.warn(err.stack));

function listPeers(client) {
  if (!client) {
    console.log(yellow('not connected'));
    return Promise.resolve();
  }
  return client.requestConfig(5000).then(peers => {
    console.log(magenta('Cluster peers:'))
    for(let id in peers.urls) {
      let url = peers.urls[id];
      if (id === peers.leaderId) {
        console.log(bgGreen(`${id}: ${url}`));
      }
      else
        console.log(`${cyan(id)}: ${grey(url)}`);
    }
  });
}

function showInfo(client, id) {
  if (!client) {
    console.log(yellow('not connected'));
    return Promise.resolve();
  }
  var urls, anyPeer = false;
  if (client.peers.has(id)) {
    urls = client.urls;
    client.setUrls(client.peers.get(id));
    anyPeer = true;
  }
  return client.requestLogInfo(anyPeer, 5000)
  .then(({isLeader, leaderId, currentTerm, firstIndex, lastApplied, commitIndex, lastIndex, snapshotSize}) => {
    if (!anyPeer) assert(isLeader);
    console.log(grey(`Log information for: "${isLeader ? green(leaderId) : yellow(id)}"`));
    console.log(`leader:          ${isLeader ? green('yes') : cyan('no')}`);
    console.log(`current term:    ${magenta(lpad(currentTerm, 14))}`);
    console.log(`first log index: ${magenta(lpad(firstIndex, 14))}`);
    console.log(`last applied:    ${magenta(lpad(lastApplied, 14))}`);
    console.log(`commit index:    ${magenta(lpad(commitIndex, 14))}`);
    console.log(`last log index:  ${magenta(lpad(lastIndex, 14))}`);
    console.log(`snapshot size:   ${magenta(lpad(snapshotSize, 14))}`);
    if (urls) client.setUrls(urls);
  });
}

function floodingNextFactory(enc, flood) {
  return function() {
    if (flood.flooding) {
      const iteration = ++flood.iteration;
      console.log('updating: %s', iteration);
      enc.encode({iteration, data: flood.data, time: new Date()});
    }
  }
}

function startFloodingClient(repl, client) {
  const lastId = Buffer.alloc(12);
  const flood = repl.context.flood;
  const entries = repl.context.entries;
  const enc = new msgpack.Encoder();
  const next = floodingNextFactory(enc, flood);

  enc.on('data', buf => {
    const id = genIdent(lastId, 0);
    client.requestUpdate(id, buf)
    .then(index => {
      entries.logIndex = index;
      // console.log('updated log index: %s', index, buf.toString('hex'));
      next();
    })
    .catch(err => replError(repl, err));
  });

  flood.flooding = true;
  next();
}

function startFloodingStream(repl, subs) {
  const flood = repl.context.flood;
  const enc = new msgpack.Encoder();
  const next = floodingNextFactory(enc, flood);

  enc.on('data', buf => {
    buf.requestId = genIdent();
    if (subs.write(buf)) {
      next();
    }
    else {
      subs.once('drain', next);
    }
  });

  flood.flooding = true;
  next();
}

function argToBoolean(arg) {
  switch(arg.toLowerCase()) {
  case 'yes':
  case 'y':
  case 'on':
  case '1':
    return true;
  default:
    return false;
  }
}

function prompt(repl) {
  repl.lineParser && repl.lineParser.reset();
  if (repl.clearBufferedCommand) {
    repl.clearBufferedCommand();
  } else {
    repl.bufferedCommand = '';
  }
  repl.displayPrompt();
};

function replError(repl, err) {
  console.warn(red('ERROR'));
  console.warn(err.stack);
  prompt(repl);
}
