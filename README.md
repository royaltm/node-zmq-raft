ØMQ Raft for nodejs
===================

An opinionated implementation of [Raft](https://raft.github.io/#implementations) consensus algorithm powered by ØMQ.

The ØMQ part is in the core of this implementation and is not replaceable.
State machine and some other parts (Log, Snapshot, Persistence) can be easily replaced though.

Components
----------

```
                             +---------+
                             | Clients |
                             +---------+
                              /|\   /|\
                             / | \ / | \
                             +---------+
                             | Cluster |
                             +---------+
                                \ | /
                                 \|/
   +-------------+         +-------------+         +----------------+
   | Persistence | ======= | Raft * peer | ======= | Log + Snapshot |
   +-------------+         +-------------+         +----------------+
                                 / \
                                /   \
                               /     \
                              /       \
                             /         \
                            /           \
              +---------------+        +---------+
              | State machine | ------ | Clients |
              +---------------+        +---------+
```
### Cluster

A group of well known and interconnected Raft peers.


### Raft peer

A Raft peer is a server with a single ZMQ_ROUTER type socket for connections incoming from clients and its peers.


### Persistence

ACID storage for the Raft's state:

- the current term,
- the last voted for,
- the cluster configuration.

The `RaftPersistence` implementation is file based.


### Log + Snapshot

ACID storage for:

- log entries
- a snapshot

`FileLog` and `SnapshotFile` implementations are file based.

See also: [ACID](ACID.md).


### State machine

The state machine is opaque to zmq-raft.
Developers should implement it following [`StateMachineBase`](lib/api/state_machine_base.js) class api.


### Clients

Clients can connect directly to any Raft peer server for:

- Retrieving the cluster configuration (cluster discovery) with REQUEST_CONFIG RPC.
- Retrieving the latest Raft state and log meta-data with REQUEST_LOG_INFO RPC.
- Retrieving log entries with REQUEST_ENTRIES RPC.
- Uploading state updates with REQUEST_UPDATE RPC.
- Requesting cluster configuration changes with CONFIG_UPDATE RPC.
- Custom requests provided by the state machine implementation.

Clients can also connect to the state machine directly depending on its implementation.

Client tools:

* [`ZmqRaftPeerClient`](lib/client/zmq_raft_peer_client.js) implements the single-peer client RPC protocol.
* [`ZmqRaftClient`](lib/client/zmq_raft_client.js) implements an easy to use cluster-aware client, w/ peer failover, configuration auto-discovery etc.


Broadcasting State Machine (BSM)
--------------------------------

This repository provides one implementation of the (state opaque) proxy state machine: `BroadcastStateMachine`.

BSM opens a ZMQ_PUB socket (when its peer is a Raft LEADER) and broadcasts applied log entries.
Clients should query zmq-raft (with REQEUST_URL rpc) for the broadcasting PUB URL.

State machine changes will be fan out to any number of clients in real time.

When clients miss some entries they have to query zmq-raft for missing entries with REQUEST_ENTRIES RPC.

Client tools:

* [`ZmqRaftPeerSub`](lib/client/zmq_raft_peer_sub.js) implements the single-peer BSM-based log entries stream reader on top of `ZmqRaftPeerClient`.
* [`ZmqRaftSubscriber`](lib/client/zmq_raft_subscriber.js) implements an easy to use cluster-aware BSM-based stream R/W client, w/ peer failover, configuration auto-discovery etc.


Hier
----

The protocol and some implementation details are presented here: [PROTO](PROTO.md) and [RAFT](RAFT.md).

Public main classes:

Assuming:

```
const raft = require('zmq-raft');
```

Server components:

- [`raft.server.ZmqRaft`](lib/server/raft.js)
- [`raft.server.FileLog`](lib/server/filelog.js)
- [`raft.common.SnaphotFile`](lib/common/snapshotfile.js)
- [`raft.server.RaftPersistence`](lib/server/raft_persistence.js)
- [`raft.server.BroadcastStateMachine`](lib/server/broadcast_state_machine.js)

Public base API for building applications:

- [`raft.api.PersistenceBase`](lib/api/persistence_base.js)
- [`raft.api.StateMachineBase`](lib/api/state_machine_base.js)

Client components:

- [`raft.client.ZmqRaftPeerClient`](lib/client/zmq_raft_peer_client.js)
- [`raft.client.ZmqRaftPeerSub`](lib/client/zmq_raft_peer_sub.js)
- [`raft.client.ZmqRaftClient`](lib/client/zmq_raft_client.js)
- [`raft.client.ZmqRaftSubscriber`](lib/client/zmq_raft_subscriber.js)

Data parsers:

- [`raft.common.LogEntry`](lib/common/log_entry.js)
- [`common.SnapshotChunk`](lib/common/snapshot_chunk.js)

Public intermediate common classes for building implementations:

- [`raft.common.ClusterConfiguration`](lib/common/cluster_configuration.js)
- [`raft.common.IndexFile`](lib/common/indexfile.js)
- [`raft.common.ReadyEmitter`](lib/common/readyemitter.js)
- [`raft.common.FilePersistence`](lib/common/file_persistence.js)
- [`raft.common.StateMachineWriter`](lib/common/state_machine_writer.js)
- [`raft.client.ZmqProtocolSocket`](lib/client/zmq_protocol_socket.js)
- [`raft.server.ZmqRpcSocket`](lib/server/zmq_rpc_socket.js)

Protocol components:

- [`raft.protocol.FramesProtocol`](lib/protocol/frames_protocol.js)
- [`raft.protocol.Protocols`](lib/protocol/index.js)

Helper utilities:

- [`raft.protocol`](lib/protocol/index.js): communication protocol frames for FramesProtocol.
- [`raft.common.constants`](lib/common/constants.js): important defaults.
- [`raft.utils.id`](lib/utils/id.js): unique ID utilities.
- [`raft.utils.helpers`](lib/utils/helpers.js): various helper functions.
- [`raft.utils.fsutil`](lib/utils/fsutil.js): file utilities.


Use cases
---------

The author envisions ØMQ Raft Server usage in two basic scenarios:

1. Scenario - in-process RAFT peer.

In this scenario, each application instance embeds a single RAFT peer instance. All application instances are run on different machines and form a RAFT cluster.

This scenario plays best if the number of server instances is uneven and predictable. Adding new server instances will require updating the RAFT configuration. This scenario provides the best state update latency.

* The application embeds the RAFT peer, e.g. using [`raft.server.builder`](lib/server/builder.js).
* The application then implements its own state machine by extending [`raft.api.StateMachineBase`](lib/api/state_machine_base.js) and providing it to the builder's `factory` option. The State Machine instance receives the committed log entries and can update itself or a local database file.
* The application utilizes the [`raft.client.ZmqRaftClient`](lib/client/zmq_raft_client.js#L409) class to update RAFT state with `requestUpdate` method.

2. Scenario - external RAFT peer.

In this scenario, the RAFT peers run as stand-alone processes e.g. using `bin/zmq-raft.js` on different machines. The RAFT peer isntances form a RAFT cluster, and applications can access it remotely through a network. The number of application instances can differ from the number of RAFT peers. The applications can come and go dynamically without impacting the RAFT configuration. The number of RAFT peers is changeable through cluster configuration change protocol.

It is the most flexible scenario, but the state update latency may be slightly worse.

* The RAFT peer servers run as standalone processes and implement the [`raft.server.BroadcastStateMachine`](lib/server/broadcast_state_machine.js) state machine.
* The application utilizes the [`raft.client.ZmqRaftSubscriber`](lib/client/zmq_raft_subscriber.js) to receive both the committed log entries and update the RAFT cluster via standard Node.JS `Duplex` stream interface.


How To
------

Building a Raft server requires to assemble component class instances for:

- The Raft Persistence.
- Log + snapshot.
- The state machine.
- The `ZmqRaft` server.

The simplest way is to use [`raft.server.builder`](lib/server/builder.js) that provides convenient defaults for all the necessary components.

This will create a single peer raft server listening on `tcp://127.0.0.1:8047` with data stored in `/tmp/raft` directory:

```js
const raft = require('zmq-raft');
raft.server.builder.build({data: {path: '/tmp/raft'}}).then(zmqRaft => {
  console.log('server alive and ready at: %s', zmqRaft.url);
});
```

The following example will create a raft server instance for the first peer in a cluster with the `BroadcastStateMachine` as its state machine:
```js
raft.server.builder.build({
  id: "my1",
  secret: "",
  peers: [
    {id: "my1", url: "tcp://127.0.0.1:8047"},
    {id: "my2", url: "tcp://127.0.0.1:8147"},
    {id: "my3", url: "tcp://127.0.0.1:8247"}
  ],
  data: {
    path: "/path/to/raft/data"
  },
  router: {
    /* optional */
    bind: "tcp://*:8047"
  },
  broadcast: {
    /* required for broadcast state */
    url: "tcp://127.0.0.1:8048",
    /* optional */
    bind: "tcp://*:8048"
  }
}).then(zmqRaft => { /* ... */ });
```

To provide a custom state machine override `factory.state` function in `builder.build` options:

```js
raft.server.builder.build({
  /* ... */
  factory: {
    state: (options) => new MyStateMachine(options);
  }
})
```

Provide your own listeners for events on the `ZmqRaft` instance instead of the default ones or disable them:
The listeners are attached early just after `ZmqRaft` is being initialized.

```js
raft.server.builder.build({
  /* ... */
  listeners: {
    error: (err) => {
      console.warn(err.stack);
    },
    state: (state, currentTerm) => {
      console.warn('state: %s term: %s', state, currentTerm);
    },
    close: null /* pass null to prevent initializing default listeners */
  }
})
```


### Quick Start Guide.

For testing, or to quickly setup the 0MQ Raft server with the Broadcasting State Machine use `bin/zmq-raft.js`:

```
  Usage: zmq-raft [options] [id]

  start zmq-raft cluster peer using provided config and optional id

  Options:

    -V, --version        output the version number
    -c, --config <file>  config file (default: config\default.hjson)
    -b, --bind <url>     router bind url
    -p, --pub <url>      broadcast state machine url
    -w, --www <url>      webmonitor url
    --ns [namespace]     raft config root namespace (default: raft)
    -h, --help           output usage information
```

e.g.:

```
export DEBUG=*
bin/zmq-raft.js -c config/example.hjson 1 &
bin/zmq-raft.js -c config/example.hjson 2 &
bin/zmq-raft.js -c config/example.hjson 3 &
```

You can direct your web browser to the webmonitor of any of the started peers:

- http://localhost:8050
- http://localhost:8150
- http://localhost:8250

To experiment with our cluster, let's spawn another terminal window and enter the `cli` with:

```
DEBUG=* npm run cli
```

1. Now, from the `cli`, let's connect to the cluster with: `.connect 127.0.0.1:8047`.
2. Let's subscribe to the BSM from another console with: `.subscribe 127.0.0.1:8047`.
3. We can now flood the cluster with some updates using: `.start some_data`. You will see the updates being populated to the subscribers.
4. To stop flooding, enter `.stop`.
5. To read the whole log, type: `.read`.
6. To get the current log information, type: `.info`.
7. Type `.help` for more commands.


### Cluster membership changes.

#### Adding new peers to the cluster.

1. Let's start the new peer (preferably from a new terminal window):

```
DEBUG=* bin/zmq-raft.js -c config/example.hjson \
  --bind "tcp://*:8347" \
  --pub tcp://127.0.0.1:8348 \
  --www http://localhost:8350 4
```

We've added some arguments that are missing in the `example.hjson` file, so the peer can setup itself properly. On production, those options should've been added to the new peer's unique configuration file.

The important part is that the new peer MUST NOT be included in the `peers` collection of the configuration file.

The new peer `4` will connect itself to the cluster as a client and fetch the current log data. Then it changes its RAFT status to `CLIENT` and opens its ROUTER socket listening for messages.

2. We will send a `ConfigUpdate RPC` to the cluster to update the peer membership for the new peer `4`. From another terminal window:

```
DEBUG=* bin/zr-config.js -c config/example.hjson -a tcp://127.0.0.1:8347/4
```

In addition to a bunch of debug messages you should also see:

```
Requesting configuration change with ...some request id...:
  tcp://127.0.0.1:8047/1
  tcp://127.0.0.1:8147/2
  tcp://127.0.0.1:8247/3
  tcp://127.0.0.1:8347/4 (added)

Cluster joined configuration changed at index ...some index....
Cluster final configuration changed at index ...some index...:
  tcp://127.0.0.1:8047/1 (leader)
  tcp://127.0.0.1:8147/2
  tcp://127.0.0.1:8247/3
  tcp://127.0.0.1:8347/4
```

The `(leader)` may appear beside a different row.

If you check out the terminal where the new peer was started, you may notice that the peer has changed its status to the `FOLLOWER`.

The web monitor should have also picked up the membership change and there should appear a new row for the new peer: `4`.

From the `cli` you may check the peers' status with the `.peer` command:

```
> .peers
Cluster peers:
1: tcp://127.0.0.1:8047
2: tcp://127.0.0.1:8147
3: tcp://127.0.0.1:8247
4: tcp://127.0.0.1:8347
```

The leader, if elected, will be highlighted.

You can further experiment with killing and restarting peers and observing the leader election process, e.g. while flooding the cluster with updates.


#### Removing peers from the cluster.

Now to remove the peer `4` from the cluster:

```
DEBUG=* bin/zr-config.js -c config/example.hjson -d tcp://127.0.0.1:8347/4
```

After the peer has been successfully removed, if it wasn't a leader during the configuration update it will most probably become a CANDIDATE. It happens when the removed peer isn't updated with the final `Cnew` configuration. This is ok, because cluster members will ignore voting requests from non-member peers. For more information on membership changes read [here](RAFT.md).


### Interpreting log entries.

The log entry format is described [here](PROTO.md). There are several API methods that provide entries as raw data Buffers. In those instances [`raft.common.LogEntry`](lib/common/log_entry.js) API can be used to interpret each entry.

Please note that not all log entries contain state data. Log entries are also created for cluster configuration changes and checkpointing (see [RAFT](RAFT.md)).

Here is an example on how to interpret log entries when extending [`raft.api.StateMachineBase`](lib/api/state_machine_base.js):

```js
class MyStateMachine extends raft.api.StateMachineBase {
    constructor() {
      super();
      initMyStateMachine().then(() => {
        this[Symbol.for('setReady')]();
      });
    }

    applyEntries(logEntries, nextIndex, currentTerm, snapshot) {
      for (let [index, item] of logEntries.entries()) {
          let entry = raft.common.LogEntry.bufferToLogEntry(item, nextIndex + index);
          console.log("log entry: log-index=%s term=%s", entry.logIndex, entry.readEntryTerm());
          if (entry.isStateEntry) {
            console.log("this is state entry:");
            //  user data of the log entry
            let data = entry.readEntryData();
            // ... do something with entry data
          } else if (entry.isConfigEntry) {
            console.log("this is config entry");
          } else {
            console.log("this is checkpoint entry");
          }
      }
      return super.applyEntries(logEntries, nextIndex, currentTerm, snapshot);
    }
}
```

An example of the "pulse" event listener of the `raft.client.ZmqRaftPeerSub` client:

```js
let sub = new raft.client.ZmqRaftPeerSub(/* ... */);
sub.on('pulse', (lastIndex, currentTerm, logEntries) => {
  let nextIndex = lastIndex - entries.length + 1;
  for (let [index, item] of logEntries.entries()) {
      let entry = raft.common.LogEntry.bufferToLogEntry(item, nextIndex + index);
      // ... do something with entry
  }
});
```

The `receiver` callback argument to `requestEntries` method of [`ZmqRaftClient`](lib/client/zmq_raft_client.js#L465) and [`ZmqRaftPeerClient`](lib/client/zmq_raft_peer_client.js#L383) also receives log entries as an array of raw Buffer chunks, that can be interpreted the same way.

The following:

* [`ZmqRaftSubscriber`](lib/client/zmq_raft_subscriber.js),
* [`RequestEntriesStream`](lib/client/zmq_raft_peer_client.js#L597) returned from [`requestEntriesStream`](lib/client/zmq_raft_client.js#L557),
* [`ForeverEntriesStream`](lib/client/zmq_raft_peer_sub.js#L404) returned from [`foreverEntriesStream`](lib/client/zmq_raft_peer_sub.js#L336)

are all object streams that yield instances of either [`common.LogEntry`](lib/common/log_entry.js) or [`common.SnapshotChunk`](lib/common/snapshot_chunk.js).

```js
let snapshot;

stream.on('data', obj => {
  console.log("received entry or chunk with index: %d", obj.logIndex);
  if (obj.isLogEntry && obj.isStateEntry) {
    let data = obj.readEntryData();
    // ... do something with a state log entry
  }
  else if (obj.isSnapshotChunk) {
    // ... do something with a snapshot chunk
    // obj.snapshotByteOffset - the byte offset of this chunk
    // obj.snapshotTotalLength - the total snapshot size in bytes
    // obj.logTerm - the snapshot log term
    // obj.length - the snapshot chunk length
    // any other Buffer instance method can be also called on obj
    if (obj.isFirstChunk) {
      snapshot = createNewSnapshot(obj.snapshotTotalLength, obj.logIndex, obj.logTerm);
    }
    // this is just an exmample, use stream piping instead or implement backpressure with pause/resume
    snapshot.write(obj, obj.snapshotByteOffset);
    if (obj.isLastChunk) {
      snapshot.commit();
    }
  }
});
```


### Updating state.

The state of the cluster can be updated using the [Request Update RPC](PROTO.md#requestupdate-rpc).

Depending on chosen scenario, use either a [`ZmqRaftClient#requestUpdate`](lib/client/zmq_raft_client.js#L409) method or use the [`stream.Writable`](https://nodejs.org/dist/latest-v18.x/docs/api/stream.html#writable-streams) side of the [`raft.client.ZmqRaftSubscriber`](lib/client/zmq_raft_subscriber.js) `Duplex` API.

Scenario [1](#use-cases):

```js
const stateMachine = new MyStateMachine(/* ... */);
const raftPeer = raft.server.builder.build({
  // ...
  factory: {
    state: (_) => stateMachine
  }
})
// ...
const seedPeers = ["tcp://raft-host-1.local:8047", "tcp://raft-host-2.local:8047", "tcp://raft-host-3.local:8047"];
// seed peers are only here for initial discovery, ZmqRaftClient retrieves the actual peer list
// from any RAFT server it connects to initially
const client = new raft.client.ZmqRaftClient(seedPeers, {
                                  secret: mySecret, lazy: true, heartbeat: 5000});
// keep the client instance through the lifetime of your application,
// so it can keep track of the cluster membership changes

async function requestUpdate(txData) {
  const serializedTxData = Buffer.from(JSON.stringify(txData));
  const requestId = raft.utils.id.genIdent();
  const logIndex = await client.requestUpdate(requestId, serializedTxData);
  console.log('state updated with log tx: %d, request-id: %s', logIndex, requestId);
  return logIndex;
} 

// sometimes your application may want to check if the current data is "fresh" by comparing the
// last log index applied to the state machine with the log index of the last known update committed.
stateMachine.lastUpdateLogIndex = 0;

requestUpdate({foo: 1}).then(index => {
  if (index > stateMachine.lastUpdateLogIndex) {
    stateMachine.lastUpdateLogIndex = index;
  }
  notifyIsFresh(stateMachine.lastApplied >= stateMachine.lastUpdateLogIndex);
});

// somewhere in your state machine implementation
class MyStateMachine extends raft.api.StateMachineBase {
    //...
    applyEntries(logEntries, nextIndex, currentTerm, snapshot) {
      // ...
      let res = super.applyEntries(logEntries, nextIndex, currentTerm, snapshot);
      notifyIsFresh(this.lastApplied >= this.lastUpdateLogIndex);
      return res;
    }
}
```

Scenario [2](#use-cases):

```js
const seedPeers = ["tcp://raft-host-1.local:8047", "tcp://raft-host-2.local:8047", "tcp://raft-host-3.local:8047"];
const sub = new raft.client.ZmqRaftSubscriber(seedPeers, {
                              secret: mySecret, lastIndex: localLastIndex||0});
// keep the sub instance through the lifetime of your application

const bufferToUpdateRequest = raft.common.LogEntry.UpdateRequest.bufferToUpdateRequest;

function requestUpdate(txData) {
  const serializedTxData = Buffer.from(JSON.stringify(txData));
  const requestId = raft.utils.id.genIdent();
  const updateRequest = bufferToUpdateRequest(serializedTxData, requestId);
  notifyIsFresh(false);
  return sub.write(updateRequest);
} 

// ...
if (requestUpdate(txData)) {
  console.log("no backpressure");
  nextIteration();
}
else {
  console.log("backpressured");
  sub.once('drain', () => nextIteration());
}

sub.on('data', obj => {
  // ...
  notifyIsFresh(obj.logIndex >= sub.lastUpdateLogIndex);
});
```
