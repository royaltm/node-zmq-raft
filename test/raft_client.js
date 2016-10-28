"use strict";

const assert = require('assert');
const os = require('os');
const fs = require('fs');
const path = require('path');
const dns = require('dns');

const zmq = require('zmq');
const colors = require('colors/safe');

const { createRepl } = require('../repl');

const { LogEntry } = require('../lib/common/log_entry');
const SnapshotFile = require('../lib/common/snapshotfile');
const ZmqRaftClient = require('../lib/client/zmq_raft_client');
const ZmqRaftSubscriber = require('../lib/client/zmq_raft_subscriber');
const FileLog = require('../lib/server/filelog');
const BroadcastStateMachine = require('../lib/server/broadcast_state_machine');
const RaftPersistence = require('../lib/server/raft_persistence');

const { genIdent, isIdent } = require('../lib/utils/id');
const msgpack = require('msgpack-lite');

var client;
var subs;

console.log("------------------------------");
console.log("zmq-raft client REPL, welcome!");
console.log("------------------------------");

dns.lookup(os.hostname(), (err, address, family) => {
  const host = family == 4 ? address : `[${address}]`;

  const url = process.argv[2] || `tcp://${host}:${((process.env.PORT || 8000) & 0xffff) + 1}`;

  createRepl().then(repl => {
    repl.on('exitsafe', () => {
      console.log('Received "exit" event from repl!');
      process.exit();
    });
    repl.on('reset', initializeContext);
    repl.defineCommand('peers', {
      help: 'Show cluster config',
      action: function(name) {
        this.lineParser.reset();
        this.bufferedCommand = '';
        listPeers().then(() => this.displayPrompt());
      }
    });

    const ben = require('ben');

    var options = {secret: process.env.SECRET || 'kiełbasa', url: url};

    function initializeContext(context) {
      client && client.close();
      subs && subs.close();

      client = new ZmqRaftClient(options);
      subs = new ZmqRaftSubscriber(Object.assign({lastIndex: 0}, options));
      subs.on('error', err => {
        console.warn("ERROR in subscriber: %s", err);
        console.warn(err.stack);
      });

      Object.assign(context, {
        colors,
        ri: repl,
        client,
        subs,
        BroadcastStateMachine,
        RaftPersistence,
        ZmqRaftSubscriber,
        LogEntry,
        SnapshotFile,
        ZmqRaftClient,
        FileLog,
        genIdent,
        isIdent,
        msgpack,
        ben,
        zmq
      });

      if (process.argv.length > 2) {
        listPeers();
      }
    }

    function listPeers()  {
      return client.requestConfig().then(peers => {
        console.log(colors.magenta(`Cluster ${colors.yellow(options.secret)} peers:`))
        for(let id in peers.urls) {
          let url = peers.urls[id];
          if (id === peers.leaderId) {
            console.log(colors.bgGreen(`${id}: ${url}`));
          }
          else
            console.log(`${colors.cyan(id)}: ${colors.grey(url)}`);
        }
      }, err => console.warn(err.stack));
    }

    initializeContext(repl.context);
  }).catch(err => console.warn(err.stack));

});

/*

subs.on('data',e=>console.log(e)).on('error',e=>console.warn(e.stack)).on('fresh',()=>console.log('FRESH')).on('stale',()=>console.log('STALE')).on('timeout',()=>console.log('TIMEOUT'));null
subs.sub.subscribe('')
subs.close()

createExampleDb = require('../../test/node-streamdb/example/cellestial');
var db;createExampleDb(true).then(o=>(db=o))
db.stream.on('error', console.warn)
subs.pipe(db.stream).pipe(subs).on('data',e=>console.log(e)).on('error',e=>console.warn(e.stack)).on('fresh',()=>console.log('FRESH')).on('stale',()=>console.log('STALE')).on('timeout',()=>console.log('TIMEOUT'));null


client.requestConfig().then(console.log,console.warn)
client.requestEntries(0, (status, entries, last) => {console.log('1 %s %s %s'); return false;}).then(console.log,console.warn)
var ent1=[];client.requestEntries(0, (status, entries, last) => {console.log('1 %s %s %s', status, entries.length, last);ent1.push(...entries)}).then(console.log,console.warn)
var ent2=[];client.requestEntries(0, (status, entries, last) => {console.log('2 %s %s %s', status, entries.length, last);ent2.push(...entries)}).then(console.log,console.warn)
var ent3=[];client.requestEntries(0, (status, entries, last) => {console.log('3 %s %s %s', status, entries.length, last);ent3.push(...entries)}).then(console.log,console.warn)


var ent=[];client.requestEntries(0, 1, (status, chunk, last, bo, bs) => {console.log('%s %s index: %s bo: %s bs: %s', status, chunk.length, last,bo,bs);ent.push(chunk)}).then(console.log,console.warn)

var ent=[];client.requestEntries(1, 1, (status, entries, last) => {console.log('%s %s index: %s', status, entries.length, last);ent.push(...entries)}).then(console.log,console.warn)
logz=ent.map(e=>new LogEntry(e))


var ent=0;ben.async(1, cb=>client.requestEntries(0, (status, entries, last) => {ent+=entries.length}).then(cb,console.warn),ms=>console.log('ms: %s %s',ms,ent));

ent.map(e=>msgpack.decode(e.slice(20)))[0]
var data = msgpack.encode({foo: 'bar'})
client.requestUpdate(genIdent(), msgpack.encode({foo: 'bar'})).then(console.log,console.warn)
client.requestLogInfo().then(console.log,console.warn)
client.requestConfig().then(console.log,console.warn)
var n=0;ben.async(50,cb=>client.requestUpdate(genIdent(), msgpack.encode({foo: 'foo',n:++n,pid:process.pid})).then(cb,console.warn),ms=>console.log('ms: %s',ms));
var i=0;ben.async(500,cb=>client.requestUpdate(genIdent(), msgpack.encode({foo: 'bar',i:++i,pid:process.pid})).then(cb,console.warn),ms=>console.log('ms: %s',ms));
var j=0;ben.async(500,cb=>client.requestUpdate(genIdent(), msgpack.encode({foo: 'baz',j:++j,pid:process.pid})).then(cb,console.warn),ms=>console.log('ms: %s',ms));


var SnapshotFile=require('../../advertine/distribute/raft/snapshotfile')
var encodeStream = msgpack.createEncodeStream(), gzip = zlib.createGzip();encodeStream.pipe(gzip);
var snapshotfile = new SnapshotFile('snap',1,1,gzip);snapshotfile.ready().then(s=>console.log(s),console.warn);
for(var line of db.createDataExporter()) {
  console.log(line); encodeStream.write(line);
}
encodeStream.end();

*/
