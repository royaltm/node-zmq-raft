Implementation
--------------

The differences of the ØMQ Raft implementation to the original RAFT proposal:

1. Raft peers only accept voteRequest, appendEntries and installSnapshot from the known peers in the configuration.
2. During network partitions candidates don't timeout for the next election term unless they receive responses for their own voteRequest messages from at least majority of the peers in the cluster.
3. During network partitions candidates ignore higher term candidates' votes unless they receive responses for their own voteRequest messages from at least majority of the peers in the cluster.
4. State machine can store permanently last applied index and on startup ØMQ Raft will assign its commit index to the state machine's last applied index.
5. ØMQ Raft peer in the follower state will refuse to truncate its log below commit index and will panic.

Additions:

### Update idempotency

The idempotency of updates is achieved by caching in memory and storing in the log, request ids sent as part of the `RequestUpdate RPC` together with the appended state. Update requests with a unique request id will be applied only once to the log as long as the request id is found in the cache. To limit the memory usage, the caching is limited in time, that is the request id consists in part of the real time signature and it will expire after a specified time. If the request id is considered not fresh the update will not be applied
and the error indicating this case will be returned to the requesting party.

The default update request expiration time is 7 hours. Thus the client have limited time to get its updates to the cluster and if that time passes the cluster peers should refuse to complete the update.

In practical measures it means that clients that randomly update the state should not be disconnected from the cluster for the longer period than the expiration time (7 hours by default).
This limit can not be disabled, but it may be extended by a large (limited to a few years) amount.

### Checkpointing

ØMQ Raft introduces 3 types of log entries: STATE, CONFIG and CHECKPOINT.

The STATE entries store the actual state data. The CONFIG entries store the updated peer configuration
when the peer membership changes. The existence of CHECKPOINT entries is explained below.

When a peer becomes the leader its RAFT log may contain uncommitted entries. In this instance the last log entry term is always lower than the current leader's term. According to the log consistency rule the leader may not commit the last entry which is not part of the current term. This would force clients to wait for another update to the state machine being made to commit all of previous log entries. In ØMQ Raft freshly elected leader can detect if this is the case and if so, the leader will apply new CHECKPOINT entry to the log with the current term (emulating an update of the RAFT state, without an actual update), allowing previous log entries to be committed ASAP. Clients and the state machine should simply ignore all of the CHECKPOINT entries when retrieving the RAFT log data.

This situation is easily demonstrable:

Let's assume we have 3 raft peers: A, B and C and A is the leader of the current term T1.

1. Shut down all followers (B and C). A(T1) is still serving clients: according to RAFT unless there is an update request the leader does not care for the state of the followers.
2. A client sends a request udpate RPC to the current leader - A(T1).
3. A leader A(T1) appends the new entry (T1) to its RAFT log and begins to replicate that entry, but since followers are down, it can't commit that entry.
4. Now stop and restart A. The peer starts as a follower A(T1) and after an election timeout it becomes a candidate A(T2).
5. Now start B and C. Due to the log completeness rule there is a very high probability that A will be elected as a leader of the new term: T2.
6. A new leader A(T2) begins successfully replicating its new log entry to B and C but it can't commit the last log entry because of the log consistency rule (T1 < T2).
7. A client is waiting (possibly forever) for its T1 update to be committed.

This is where ØMQ Raft comes to the rescue:

8. The peer A(T2) when elevated as a leader will check if its last log term is lower than the current term and its commit index is lower than the last log index. If so, A(T2) appends a new CHECKPOINT entry (T2) and replicates the new entry to followers B and C.
9. Now A(T2) can commit the CHECKPOINT entry successfully and in the process (RAFT's log completeness rule) also any previous uncommitted log entries (T1).


Updating cluster configuration (peer membership)
------------------------------------------------

ØMQ Raft implements cluster peer membership changes with transitional stage as proposed in RAFT using `ConfigUpdate RPC`.

To change the configuration of the cluster always follow this 3-step procedure:

1. Start ØMQ Raft peers that are being added to the cluster (new peers), with the current cluster configuration as their initial peer config. DO NOT include them in the `peers` field of the config. Peers that are not part of the cluster config will start in the CLIENT state. In this state peers will connect to the current cluster and will download the current snapshot and log using `RequestEntries RPC` up to the last committed entry. Once the transfer is finished they will open the router socket and will start to listen to append entries and vote requests RPCs. While in this state they will never promote themselves to CANDIDATEs but will vote in elections and replicate the log from the current leader.
2. Send the updated cluster configuration to the current cluster, e.g. using `zr-config` tool or from cli or any other program using `ConfigUpdate RPC`. Once the leader replicates the transitional `Cold,new` CONFIG log entry to the new peers in the CLIENT state, those peers will update their cluster configuration and if they are included in the `Cnew` configuration, they will become FOLLOWERs. From that moment new peers can participate fully in elections. Once the joint majority of peers replicates the transitional config and the current leader commits its log, the LEADER will update its configuration to its final form `Cnew` and will start updating peers only found in the new configuration. Once the log is replicated to all of the followers in the final configuration, from the cluster perspective the procedure ends. Peers that are no longer part of the new configuration will retain transitional config and after election timeout they will try to start new elections. Because peers in the new configuration will ignore their vote requests, the old peers will never be able to elect a leader. Yet they will serve clients, sending them the transitional peer configuration if requested. This way clients will eventually find out about new peers in the `Cold,new` cluster configuration.
3. Before shutting down peers that are no longer part of the new cluster, make sure all the clients are aware of the changes. This is especially important when the whole cluster is being replaced with a completely new set of peers. This is safe as long as you make sure all clients had enough time to pick up the new peers' urls from the old servers.

__Important__: Make sure each new peer added to the cluster have a globally unique id. Consider using uuid however usually host fqdn or peer url are also reasonable choices.

The helper tool `zr-config` may be used to safely update the cluster membership.

Log compaction
--------------

The ØMQ Raft’s `FileLog` class provides a way to safely install a compaction snapshot. It’s up to the state machine implementation to provide the data for the snapshot.

There can be many techniques how to approach this in detail. However one of the first choices to be made is a selection of the LOG INDEX up to which the log will be compacted. The hard rule is to compact log up to the last applied index in the state machine. This basically means that the state machine creates the snapshot from its current state. 

There are however circumstances which suggest this rule is only safe in accordance with the RAFT protocol but doesn't include the preservation of the update requests' idempotency. Let's imagine that there is some buggy or partitioned client that is still waiting for the confirmation of its update request long after it has been added and committed to the log, repeating its requests endlessly. If we were to remove log entries that contained its updated entry we would also have removed the information about the request id that updated this entry, loosing in the process the necessary information for that update request to be idempotent. If we did delete the log files and somehow the client would finally got through with its RPC message, the cluster would treat this update as a completely new request.
This is partially true because peers possess the cache of request ids in memory, however in case the leader was restarted after deleting the log entries the above hypothetical situation could become true.

The above problem is partially addressed in a way `FileLog` initializes itself in R/W mode. Upon identifying the snapshot's LOG INDEX it sets its `firstIndex` property to the one following the snapshot's LOG INDEX, but nevertheless it scans all existing files in the RAFT LOG directory and builds the request id cache in memory from all of the log entries including entries preceding the snapshot's LOG INDEX. The expired request ids are not being added to the cache but instead are being counted. This procedure is performed on each log file backwards reading from the last log entry down to the first available log entry and is stopped once all the files has been scanned or the number of expired ids exceeds the capacity of the single log file, thus assuming that all previous log files contains already expired request id entries. The log entry index that is below the last known unexpired request id is called the PRUNE LOG INDEX and can be retrieved from the server using `RequestLogInfo RPC`. It's being dynamically updated when the cached request ids expire.

Knowing this one could argue that it's still safe to create a compact snapshot up to the last applied index or even the commit log (assuming an external log parser would be used) and delete the log files only if the entirety of the file's entries are below or equal to the PRUNE LOG INDEX.

This is, again, not entirely true. Let's imagine a circumstance in which some of the cluster peers' data was deleted or a new peer was added to the cluster. Of course those peers will recreate the log files via cluster `AppendEntries RPC` messages, but they will only receive log entries succeeding the snapshot's LOG INDEX. From the RAFT standpoint the log entries before the snapshot's LOG INDEX do not exist. If one of the new peers would become a leader, again we are loosing information.

Thus the safest way to select the LOG INDEX of the compaction snapshot is using the following formula:

```
MIN(COMMIT INDEX, PRUNE INDEX)
```

For example when compacting from the process where the state machine resides:

```js
// instead of commitIndex a lastApplied property can be used which may be more convenient
// lastApplied asynchronously lags behind commitIndex on frequent updates but should be close to it
var compactionIndex = Math.min(raft.commitIndex, raft.pruneIndex);
```

or when compacting a log using an external program:

```js
var compactionIndex = await client.requestLogInfo(true)
                            .then(({commitIndex, pruneIndex}) => Math.min(commitIndex, pruneIndex));
```

The second detail is that the file with a snapshot should be build first and then replaced atomically.

This can be approached in at least two ways:

From within the ØMQ Raft process. In this instance use `fileLog.installSnapshot` method after creating snapshot file with the `fileLog.createTmpSnapshot`. E.g:

```js
var compactionTerm = async fileLog.termAt(compactionIndex);
var readStream = statemachine.createSnapshotReadStream();
var snapshot = fileLog.createTmpSnapshot(compactionIndex, compactionTerm, readStream);
var filename = async fileLog.installSnapshot(snapshot, true);
```

From the external process. In this instance when initializing `FileLog` for ØMQ Raft process invoke `fileLog.watchInstallSnapshot` method first (or pass `data.compact.watch=true` config to the builder). An external program should prepare the snapshot file from the log files. When it's ready, rename the file to the path specified in the `watchInstallSnapshot` method. There is a `zr-log-compact` tool provided that can help with this task. The directory watcher in the ØMQ Raft process should pick the file up and install it automatically.

Of course the most obvious way would be to stop the peer, do the compaction and restart the peer, but would you really?

Regardless of the compaction method chosen, the old snapshot file is always being backed up by appending a suffix to the name of the old snapshot file and no files are being automatically deleted from the current log.

If you really need to delete the files, do this on your own accord.

The tool `zr-log-compact` can list the files that could be safely deleted.

### How to use `zr-log-compact`

Provide a config file: `zr-log-compact -c path/to/config` ensuring at least the following fields has been configured:

```
raft.id: the id of peer to compact
raft.peers: the cluster's peers
raft.secret: the cluster secret
raft.data.path: path/to/raft/data
raft.data.compact.install: compact_watch_dir/snap.new
raft.data.compact.state.path: path/to/state_machine
```

or provide options: `-d path/to/raft/data -t compact_watch_dir/snap.new -m path/to/state_machine`
and one of: `-i compaction_index` or: `-p peer_url -k secret`.

The state machine should be exported as a module. Additionally to implementing the `StateMachineBase` it should also expose one of the instance methods:

- `createSnapshotReadStream()` should return an object implementing the `stream.readStream` protocol that will produce the snapshot's content.
- `serialize()` should return a `Buffer` instance with the snapshot content.

The state machine will be fed with the content of the previous snapshot and all log entries up to the compaction index. Then one of the above methods will be called and the snapshot file will be created.

Alternatively the state machine can expose a `snapshotReadStream` property when initialized which should
be an object implementing the `stream.readStram` protocol. In this instance the snapshot file will be created while the state machine is being fed with the log content.

By default the state machine is initialized with the following options:

```
compressionLevel: zlib.Z_BEST_COMPRESSION
unzipSnapshot: true
```

The option `-z level` modifies the `compressionLevel`.
If the option `-z 0` is provided the `unzipSnapshot` option is not provided unless `-U` forces `unzipSnapshot: false`.

The `-N` option prevents printing of log file names to be safely deleted.


### How to use `zr-config`

Provide a config file: `zr-config -c path/to/config` which is being used to find the cluster through seed `raft.peers` and optionally a `raft.secret` unless provided with the `-k secret` option.

To add new peers to the cluster use the `-a url1,url2,...` option.
To delete some peers, use the `-d url1,url2,...` option.

Alternatively provide the complete list of new peers with the `-r url1,url2,url3` option. The current peers that are not provided after the `-r` option will be deleted.

If the peer id equals to the peer's url, just provide an url, e.g.: `tcp://1.2.3.4:8047`.
To specify a peer id use the pathname: `tcp://127.0.0.1:8047/foo` where `foo` is an id of the peer and `tcp://127.0.0.1:8047` is its url.

You can dry-run the `zr-config` with the `-n` option. The `-t msecs` can be provided to timeout before the connection to the cluster could be established.
