Implementation details
----------------------

ØMQ Raft implementation differences to the original RAFT proposal:

1. Raft peers only accept voteRequest, appendEntries and installSnapshot from the known peers in the configuration.
2. During network partitions candidates don't timeout for the next election term unless they receive responses for their own voteRequest messages from at least majority of the peers in the cluster.
3. During network partitions candidates ignore higher term candidates' votes unless they receive responses for their own voteRequest messages from at least majority of the peers in the cluster.
4. State machine can store permanently last applied index and on startup ØMQ Raft will assign its commit index to the state machine's last applied index. ØMQ Raft while in follower state will refuse to truncate its log below commit index and will panic.

Additions:

#### Update idempotency

The idempotency of updates is achieved by (limited) tracking of request ids sent as a part of RequestUpdate request messages. It is guaranteed that the request with a unique request id will be applied only once to the log.
The tracking is limited in time, that is the request id consists in part of the real time signature and
it will expire after a specified time. If the request id is considered not fresh the update will not be applied
and the error indicating this case will be returned to the requesting party.
The default update request expiration time is 7 hours. Thus the client have limited time to get its updates to the cluster and if that time passes it should refuse to complete the update.
In practical measures it means that clients that randomly updates the state should not be disconnected from cluster for the longer period than expiration time (7 hours by default).
One can not disable this time limit, but may extend it by a large (limited to a few years) amount.

#### Checkpointing

ØMQ Raft introduces 3 types of log entries: STATE, CONFIG and CHECKPOINT.
When the peer becomes a leader it might have uncommitted entries in its log and the last log entry term is lower than the current term. According to the log consistency rule it may not commit the last entry which is not part of the current term. This situation forces clients to wait for another update to the state machine to commit all previous log entries. In ØMQ Raft freshly elected leader can detect this and in such circumstances it will apply new checkpoint entry to the log with the current term, allowing previous log entries to be committed ASAP.

This situation is easily demonstrable:

Let's assume we have 3 raft peers: A, B and C and A is a current leader in term T1.

1. shut down all followers (B and C). A(T1) is still serving clients (according to RAFT unless there is an update request the leader does not care for the state of the followers)
2. client sends request udpate to current leader - A(T1)
3. Leader A(T1) appends new entry to its log and tries to replicate that entry, but since followers are out, it can't commit.
4. Stop and restart A. A starts as a follower (T1) and after election timeout it becomes a candidate (T2).
5. Start B and C. Due to the log completeness rule there is a very high probability that A will be elected as a leader of the new term T2.
6. Leader A(T2) begins successfully replicating its new log entry to B and C but can't commit its last log entry(T1 < T2). 7. Client is waiting (possibly forever) for the update to be committed.

ØMQ Raft to the rescue:

8. A(T2) on becoming a leader will check if its last log term < current term and commit index < last log index. If so, it appends a new checkpoint entry (T2) and replicates the new entry to followers.
9. Now A(T2) can commit checkpoint entry successfully and in the process (log completeness rule) also previous uncommitted log entries (T1).


Cluster configuration change (peer membership)
----------------------------------------------

ØMQ Raft implements cluster peer membership change with transitional stage as proposed in RAFT using configUpdate RPC.
There is however one difference to the RAFT proposal config change procedure. In the original paper there is a moment
in time when it's possible that the current leader is replicating log and not be a part of the new cluster configuration.
In ØMQ Raft implementation the leader instead steps down just after Cold,new is committed.
In my opinion it simplyfies finite state machine in exchange for possibly longer configuration change procedure.

To change the configuration of the cluster always follow this 3-step procedure:

1. Start raft peers that are being added to the cluster (new peers), with current cluster configuration as their initial peer config. DO NOT include them in the peer cluster config yet. Peers that are not part of the cluster will start in the CLIENT state. In this state peers will connect to the current cluster and will download the current snapshot and log using requestEntries RPC up to the last committed entry. Once the transfer is finished they will open the router socket and will start to listen to append entries and vote requests RPCs. While in this state they will never promote themselves to CANDIDATEs but will vote in elections and replicate the log from the current leader.
2. Send the updated cluster configuration to the current cluster, e.g. using zmq-raft-config tool or from cli or any other program using ConfigUpdate RPC. Once the leader replicates the transitional config log entry to the new peers in CLIENT state they will update their cluster configuration (transitional) and if they are included in the new configuration, they will become FOLLOWERs. From that moment new peers can participate fully in elections. Once the joint majority of peers replicates the transitional config and the current leader commits its log, the LEADER will update its configuration to its final form and will start updating peers only found in the new configuration. Once the log is replicated to all the followers in the final configuration, from the cluster perspective the procedure ends. Peers that are no longer part of the new configuration will retain transitional config and after election timeout they will try to start new elections. Because peers in the new configuration will ignore their vote requests, the old peers will never be able to elect a leader. Yet they will serve clients, still providing them the transitional peer configuration if requested. This way clients will eventually find out about new peers in the new cluster configuration.
3. Before shutting down peers that are no longer part of the new cluster, make sure all clients are aware of the changes. This is especially important when the whole cluster is replaced with completely new peers. This is safe as long as you make sure all clients can have enough time to pick up new peer configuration from the old servers.

__Important__: Make sure each new peer added to the cluster have globally unique id. Consider using uuid however usually host fqdn or peer url are also good choices.

The helper tool `zr-config` is provided that takes care of updating the cluster membership via ConfigUpdate RPC.

Log compaction
--------------

Zmq raft’s FileLog class allows to install compaction snapshot. It’s up to the state machine implementation to provide the data for the snapshot.
There can be many techniques how to approach this in detail, never the less there are simple rules guiding how to choose the last compacted log index safely. The hard rule is to compact log up to the last applied index in the state machine. This basically means that the state machine creates the snapshot with its current state. There are however circumstances involving update request idempotency which suggest another (let’s call it “soft”) rule. The safest way is to compact log only before the first index whose request id associated with it, is still fresh (unexpired). This can be determined using min(raft.commitIndex, FileLog.getFirstFreshIndex() - 1) formula or using requestLogInfo RPC response’s min(commit index, prune log index) when compacting log using logcompact tool or any external program.

- From within the zmq raft process. In this instance use FileLog.installSnapshot method after creating snapshot file with the FileLog.createTmpSnapshot.
- From the external process. In this instance when initializing FileLog in zmq raft process invoke FileLog.watchInstallSnapshot method (or pass `data.compact.watch=true` config to builder). External program should prepare the snapshot file and when it's ready rename it to the path specified in the watchInstallSnapshot method. There is a `zr-log-compact` tool provided that can help with this task. The directory watcher in the zmq raft process should pick the file up and FileLog will install it automatically as its new snapshot.

Regardless of the compaction method, old snapshot file is always being backed up and no files are being automatically deleted from the current log. It is however possible to prune the log. Pruning operation will delete the first N log index files providing that all the entries that these files consist of are included in the current compaction snapshot. Log entries may be archived e.g. for debugging purposes prior to pruning.


Regardless of the compaction method, old snapshot file is always being backed up and no files are being automatically deleted from the current log. It is however possible to prune the log. Pruning operation will delete the first N log index files providing that all the entries that these files consist of are included in the current compaction snapshot. Log entries may be archived e.g. for debugging purposes prior to pruning. The tool logcompact can list the files that could be pruned or can delete them optionally. (TODO)
