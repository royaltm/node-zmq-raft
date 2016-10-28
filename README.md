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

Clients can connect to state machine to retrieve current state directly or via raft server (via custom message type).

A `ZmqRaftClient` class implements an easy to use rpc client socket.

Broadcasting state machine
--------------------------

This repository includes one implementation of (state opaque) proxy state machine: `BroadcastStateMachine`.

This implementation opens a ZMQ_PUB socket (when its peer is a leader) and broadcasts applied log entries.
Clients shouls query raft (with REQEUST_URL rpc) for its broadcast url.

This will deliver state machine changes to any number of clients in real-time.
When clients miss some entries they have to query raft server directly for
missing entries with REQUEST_ENTRIES rpc.

This is implemented in `ZmqRaftSubscriber` class.

Usage
-----

TODO: write some docs actually...


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
