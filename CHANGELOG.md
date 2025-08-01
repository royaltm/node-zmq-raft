0.7.0

* tap bumped to ^20.0.3
* msgpack-lite dependency replaced with @royaltm/msgpack-lite 0.1.27
* Improved tail commit distribution latency by introducing slow peer heartbeat flag, cleared on commit.
  Once the leader commit index is updated, all peers are updated ASAP instead of on the next heartbeat.

0.6.0

* bumped zeromq ~5.3.1, commander ^11.1, tap 16.3.9, js-yaml ^4.1.0, mkdirp ^3.0.1
* remove unnecessary files from package

0.5.0

* ZmqRaftSubscriber property `lastUpdateLogIndex` added to keep track of the latest updates committed.
* tap bumped to 16.3.4 to fix multiple high security vulnerabilities.
* exchanged multiple deprecated test functions for their non-deprecated counterparts.
* fixed cli flag parsing for files in the bin folder.

0.4.1

* ZmqRaftPeerClient: (fix) RequestEntriesStream gets destroyed on request entries RPC error.
* ZmqRaftPeerSub: (fix) ForeverEntriesStream prevents recreating missing entries stream on stream end.

0.4.0

* BroadcastStateMachine: always announces its own PUB URL, regardless of the RAFT state.
* ZmqRaftPeerClient: a single RAFT peer client, specializing in the 0MQ RAFT protocol.
* ZmqRaftClient: cluster aware client is now built on top of ZmqRaftPeerClient.
* ZmqRaftPeerSub: a single RAFT peer BroadcastStateMachine client.
* bin/console.js: new commands: `.cpeer` and `.subp`.
* ZmqRaftMonitor: urlsOnly option.
* bin/zmq-monitor.js: new commandline option switches: `-u` and `-p`.

0.3.0

* bin/console.js: external config facility.
* ZmqRaftMonitor: an utility introduced in the "utils.monitor" namespace.
* bin/zmq-monitor.js: console UI for ZmqRaftMonitor.
* ZmqRaft+config: `preventSpiralElections` option introduced.
* FileLog+config: `requestIdCacheMax` introduced to set the high water mark for request-ID cache.
* FileLog+ZmqRaft+config: `requestIdTtl` option can be set to `null` to disable completely time-based checks.
