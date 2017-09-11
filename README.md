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

A group of well known and interconnected raft peers.


### Raft peer

A Raft peer is a server with a single ZMQ_ROUTER type socket for connections incoming from clients and its peers.


### Persistence

ACID storage for raft internal state:

- current term
- voter for
- cluster config 

`RaftPersistence` class implementation is file based.


### Log + Snapshot

ACID storage for:

- log entries
- snapshot

`FileLog` and `SnapshotFile` class implementations are file based.

### State machine

State machine is opaque to zmq-raft.
Developers should implement it following `StateMachineBase` class api.


### Clients

Clients can connect directly to raft server for:

- retrieving cluster config (with REQUEST_CONFIG rpc)
- retrieving raft state info (with REQUEST_LOG_INFO rpc)
- requesting log updates (with REQUEST_UPDATE rpc)
- retrieving log entries (with REQUEST_ENTRIES rpc)

Clients can connect to state machine to retrieve current state directly or via raft server (custom rpc type).

A `ZmqRaftClient` class implements an easy to use rpc client socket.

Broadcasting state machine
--------------------------

This repository includes one implementation of (state opaque) proxy state machine: `BroadcastStateMachine`.

Broadcasting state machine opens a ZMQ_PUB socket (when its peer is a leader) and broadcasts applied log entries.
Clients should query zmq-raft (with REQEUST_URL rpc) for broadcast url.

State machine changes will be fan out to any number of clients in real-time.

When clients miss some entries they have to query zmq-raft for missing entries with REQUEST_ENTRIES rpc.

This is implemented in `ZmqRaftSubscriber` class.

Hier
----

Public main classes:

Assuming:

```
const raft = require('zmq-raft');
```

- `raft.server.ZmqRaft`
- `raft.server.FileLog`
- `raft.common.SnaphotFile`
- `raft.server.RaftPersistence`
- `raft.server.BroadcastStateMachine`

- `raft.common.LogEntry`

- `raft.client.ZmqRaftClient`
- `raft.client.ZmqRaftSubscriber`

- `raft.protocol.FramesProtocol`
- `raft.protocol.Protocols`

Public intermediate common classes for building implementations:

- `raft.common.IndexFile`
- `raft.common.ReadyEmitter`
- `raft.common.FilePersistence`
- `raft.client.ZmqProtocolSocket`
- `raft.server.RpcSocket`

Public base api classes for building implementations:

- `raft.api.LogBase`
- `raft.api.SnaphotBase`
- `raft.api.PersistenceBase`
- `raft.api.StateMachineBase`
- `raft.client.ZmqSocketBase`

Helper utilities:

- `raft.protocol`: communication protocol frames for FramesProtocol
- `raft.common.constants`: important defaults
- `raft.utils.id`: unique id utilities
- `raft.utils.helpers`: various helper functions
- `raft.utils.fsutil`: various file utils

Usage
-----

Building raft server requires to assemble component class instances for:

- raft persistence
- log + snapshot
- state machine
- raft server

The simplest way is to use `raft.server.build` function which provides convenient defaults.

This will create a single peer raft server listening on `tcp://127.0.0.1:8047` with data stored in `/tmp/raft` directory:

```js
const raft = require('zmq-raft');
raft.server.build({data: {path: '/tmp/raft'}}).then(zmqRaft => {
  console.log('server alive and ready at: %s', zmqRaft.url);
});
```

The following example will create a raft server instance for the first peer in a cluster with BroadcastStateMachine as a state machine:
```js
raft.server.build({
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

To provide custom state machine override `factory.state` function in `raft.server.build` options:

```js
raft.server.build({
  /* ... */
  factory: {
    state: (options) => new MyStateMachine(options);
  }
})
```

Provide your own listeners for events on the raft instance instead of the default ones or disable them:
The listeners are attached early just after `ZmqRaft` is initialized.

```js
raft.server.build({
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


For testing, to quickly setup a raft server with broadcast state machine use `bin/zmq-raft.js`:

```
  Usage: zmq-raft [options] [id]


  Options:

    -V, --version        output the version number
    -c, --config <file>  Config file
    -h, --help           output usage information
```

e.g.:

```
export DEBUG=*
bin/zmq-raft.js -c config/example.hjson 1 &
bin/zmq-raft.js -c config/example.hjson 2 &
bin/zmq-raft.js -c config/example.hjson 3 &
```
