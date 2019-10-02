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

See also: [ACID](ACID.md).


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

The protocol and some implementation details are presented here: [PROTO](PROTO.md) and [RAFT](RAFT.md).

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

- `raft.common.ClusterConfiguration`
- `raft.common.IndexFile`
- `raft.common.ReadyEmitter`
- `raft.common.FilePersistence`
- `raft.common.StateMachineWriter`
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

To provide a custom state machine override `factory.state` function in `raft.server.build` options:

```js
raft.server.build({
  /* ... */
  factory: {
    state: (options) => new MyStateMachine(options);
  }
})
```

Provide your own listeners for events on the `ZmqRaft` instance instead of the default ones or disable them:
The listeners are attached early just after `ZmqRaft` is being initialized.

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

See [builder](lib/server/builder.js).


### Quick Start Guide.

For testing, to quickly setup a raft server with a broadcast state machine use `bin/zmq-raft.js`:

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

You can direct your browser to the webmonitor of any of the started peers:

- http://localhost:8050
- http://localhost:8150
- http://localhost:8250

To experiment with our cluster, let's spawn another terminal window and enter the `cli` with:

```
DEBUG=* npm run cli
```

1. Now, from the `cli`, let's connect to the cluster with: `.connect 127.0.0.1:8047`.
2. Let's subscribe to the state machine from another console with: `.subscribe 127.0.0.1:8047`.
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
