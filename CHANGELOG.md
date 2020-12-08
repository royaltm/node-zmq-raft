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
