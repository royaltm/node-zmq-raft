"use strict";

const assert = require('assert');
const os = require('os');
const fs = require('fs');
const path = require('path');
const dns = require('dns');

const zmq = require('zeromq');
const colors = require('colors/safe');

const raft = require('..');
const { SnapshotChunk } = raft.common;

const { createRepl } = require('../repl');

const { LogEntry, UpdateRequest } = require('../lib/common/log_entry');
const SnapshotFile = require('../lib/common/snapshotfile');
const ZmqRaftClient = require('../lib/client/zmq_raft_client');
const ZmqRaftSubscriber = require('../lib/client/zmq_raft_subscriber');
const FileLog = require('../lib/server/filelog');
const BroadcastStateMachine = require('../lib/server/broadcast_state_machine');
const RaftPersistence = require('../lib/server/raft_persistence');

const { lpad } = require('../lib/utils/helpers');
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
      action: function() {
        listPeers().then(() => {
          this.lineParser.reset();
          this.bufferedCommand = '';
          this.displayPrompt()
        });
      }
    });
    repl.defineCommand('info', {
      help: 'Show log information of a specified peer or all of them',
      action: function(id) {
        showInfo(id).then(() => {
          this.lineParser.reset();
          this.bufferedCommand = '';
          this.displayPrompt()
        });
      }
    });

    const ben = require('ben');

    var options = {secret: process.env.SECRET || 'kiełbasa', url: url};

    function initializeContext(context) {
      client && client.close();
      subs && subs.close();

      client = new ZmqRaftClient(Object.assign({lazy: true}, options));
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
        UpdateRequest,
        SnapshotChunk,
        SnapshotFile,
        ZmqRaftClient,
        FileLog,
        genIdent,
        isIdent,
        msgpack,
        ben,
        zmq,
        raft
      });

      if (process.argv.length > 2) {
        listPeers();
      }
    }

    function listPeers() {
      return client.requestConfig(5000).then(peers => {
        console.log(colors.magenta(`Cluster ${colors.yellow(options.secret)} peers:`))
        for(let id in peers.urls) {
          let url = peers.urls[id];
          if (id === peers.leaderId) {
            console.log(colors.bgGreen(`${id}: ${url}`));
          }
          else
            console.log(`${colors.cyan(id)}: ${colors.grey(url)}`);
        }
      }, err => console.log(colors.red(`ERROR: ${err}`)));
    }

    function showInfo(id) {
      var urls, anyPeer = false;
      if (client.peers.has(id)) {
        urls = client.urls;
        client.setUrls(client.peers.get(id));
        anyPeer = true;
      }
      return client.requestLogInfo(anyPeer, 5000)
      .then(({isLeader, leaderId, currentTerm, firstIndex, lastApplied, commitIndex, lastIndex, snapshotSize}) => {
        if (!anyPeer) assert(isLeader);
        console.log(colors.grey(`Log information for: "${isLeader ? colors.green(leaderId) : colors.yellow(id)}"`));
        console.log(`leader:          ${isLeader ? colors.green('yes') : colors.cyan('no')}`);
        console.log(`current term:    ${colors.magenta(lpad(currentTerm, 14))}`);
        console.log(`first log index: ${colors.magenta(lpad(firstIndex, 14))}`);
        console.log(`last applied:    ${colors.magenta(lpad(lastApplied, 14))}`);
        console.log(`commit index:    ${colors.magenta(lpad(commitIndex, 14))}`);
        console.log(`last log index:  ${colors.magenta(lpad(lastIndex, 14))}`);
        console.log(`snapshot size:   ${colors.magenta(lpad(snapshotSize, 14))}`);
        if (urls) client.setUrls(urls);
      })
      .catch(err => console.log(colors.red(`ERROR: ${err}`)));
    }

    initializeContext(repl.context);
  }).catch(err => console.warn(err.stack));

});

/*
function show(x) {console.log(x);return x;}

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

var data = msgpack.encode({foo: 'bar'}),idreq=Buffer.alloc(12);
ben.async(100, cb=>client.requestUpdate(genIdent(idreq),data).then(cb,console.warn),ms=>console.log('ms: %s',ms));

function show(x) {console.log(x);return x;}
[
"581d4a5c26f27e27889d4e8f",
"581d4a5c26f27e27889d4e90",
"581d4a5c26f27e27889d4e91",
"581d4a5c26f27e27889d4e92",
"581d4a5c26f27e27889d4e93",
"581d4a5c26f27e27889d4e94",
"581d4a5c26f27e27889d4e95",
"581d4a5c26f27e27889d4e96",
"581d4a5c26f27e27889d4e97",
"581d4a5c26f27e27889d4e98",
].forEach((req,i) => client.requestUpdate(show(req),msgpack.encode({foo: 'bar', i:i})).then(idx=>console.log('ok %s',idx),console.warn));
for(var i=0; i < 10; ++i) console.log(`"${genIdent()}",`);


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



{ x: 1206513, y: 1196252, z: 1196015, v: 5 }

function show(x) {console.log(x);return x;}
var z=451058,sendmore=()=>{subs.write(new UpdateRequest(msgpack.encode({ka:'rafa',z:++z,pid:process.pid,time:new Date().toJSON()}),show(genIdent())))&&setTimeout(sendmore,(Math.random()*500>>>0))||subs.once('drain',sendmore);}

var sizer = (entries) => entries.reduce((a,e) => a + e.length, 0);
client.requestEntries(1, (status, entries, last) => {console.log('%s count: %s size: %s last index: %s', status, entries.length, sizer(entries), last);}).then(console.log,console.warn)

var state={x:0,y:0,z:0,v:0};
function verify(chunk) {
  if (chunk.isSnapshotChunk) console.log('snapshot: (%s) index: %s bytes: %s/%s', chunk.length, chunk.logIndex, chunk.snapshotByteOffset, chunk.snapshotTotalLength);
  else {
    assert(chunk.isLogEntry);
//    console.log('entry: (%s) type: %s term: %s index: %s', chunk.length, chunk.entryType, chunk.readEntryTerm(), chunk.logIndex);
    var d = msgpack.decode(chunk.readEntryData());
    if (d && 'object' === typeof d) {
      var {x,y,z,v} = d;
      if (d.ka==='rafa') {
        if ('x' in d) { console.log('x=%s',x); assert(x===state.x+1); state.x=x; }
        if ('y' in d) { console.log('y=%s',y); assert(y===state.y+1); state.y=y; }
        if ('z' in d) { console.log('z=%s',z); assert(z===state.z+1); state.z=z; }
        if ('v' in d) { console.log('v=%s',v); assert(v===state.v+1); state.v=v; }
        return;
      }
    }
    else console.log('unknown: %j', d);
  }
}
subs.on('data',verify).on('error',e=>{console.warn(e.stack);process.exit(1)}).on('fresh',()=>console.log('FRESH')).on('stale',c=>console.log('STALE %s', c)).on('timeout',()=>console.log('TIMEOUT'));null
subs.pause();
subs.resume();
*/
