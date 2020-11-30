0.2.2

* bin/console.js: external config facility.
* ZmqRaftMonitor: an utility introduced in the "utils.monitor" namespace.
* bin/zmq-monitor.js: console UI for ZmqRaftMonitor.
* ZmqRaft+config: `preventSpiralElections` option introduced.
* FileLog+config: `requestIdCacheMax` introduced to set the high water mark for request-ID cache.
* FileLog+ZmqRaft+config: `requestIdTtl` option can be set to `null` to disable completely time-based checks.
