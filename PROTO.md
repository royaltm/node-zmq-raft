ØMQ RAFT protocol
=================

ØMQ RAFT protocol messages follow the ØMQ standard and consist of ØMQ [frames](https://rfc.zeromq.org/spec:23/ZMTP). http://zguide.zeromq.org/page:all#ZeroMQ-is-Not-a-Neutral-Carrier

Frame data types
----------------

Each frame of the protocol messages described below is being encoded as one of the following type:

* `bytes` - An array of bytes containing zero or more bytes.
* `string` - An UTF-8 encoded string of zero or more characters (no null termination).
* `uint` - An unsigned, least significant byte first, variable length integer (1 - 8 bytes), empty frame is interpreted as a protocol error.
* `nuint` - An unsigned, least significant byte first, variable length integer (1 - 8 bytes), empty frame is interpreted as `null`.
* `uint32` - An unsigned, least significant byte first, variable length integer (1 - 4 bytes), empty frame is interpreted as a protocol error.
* `bool` - A boolean. `true`: body must have at least 1 byte and the first byte must not be 0, `false` otherwise.
* `json` - A `MessagePack` encoded `JSON` data: http://msgpack.org
* `entry` - A RAFT log entry (see below).
* `reqid` - A 12 byte unique request id.

The 12-byte `reqid` is constructed as follows:

- 4-byte value representing the seconds since the Unix epoch (most significant byte first),
- 3-byte machine identifier,
- 2-byte process id,
- 3-byte counter, starting with a random value.

The RAFT log's `entry` data is constructed as follows:

- 12-bytes `reqid`,
- 1-byte log entry type: 0 - STATE, 1 - CONFIG, 2 - CHECKPOINT (see: [RAFT](RAFT.md)),
- 7-byte log entry term (least significant byte first),
- arbitrary log data.

### Examples:

```
zmq frame body as a string: "foo"

offs.| value
   0 | 0x66
   1 | 0x6f
   2 | 0x6f

zmq frame body as an uint: 255

offs.| value
   0 | 0xff

zmq frame body as an uint: 256

offs.| value
   0 | 0x00
   1 | 0x01

zmq frame body as an uint: 9007199254740991

offs.| value
   0 | 0xff
   1 | 0xff
   2 | 0xff
   3 | 0xff
   4 | 0xff
   5 | 0xff
   6 | 0x1f

zmq frame body as a bool: False

offs.| value
(empty frame)


zmq frame body as a bool: True

offs.| value
   0 | 0x01

zmq frame body as a json: null

offs.| value
   0 | 0xc0

zmq frame body as a json: [42, "foo", false]

offs.| value
   0 | 0x93
   1 | 0x2a
   2 | 0xa3
   3 | 0x66
   4 | 0x6f
   5 | 0x6f
   6 | 0xc2

zmq frame body as a log entry: [5956dc8826f27e10dcccab20, LOG_ENTRY_TYPE_STATE, 42, "foo"]

offs.| value
   0 | 0x59 - 12 byte request id
   1 | 0x56
   2 | 0xdc
   3 | 0x88
   4 | 0x26
   5 | 0xf2
   6 | 0x7e
   7 | 0x10
   8 | 0xdc
   9 | 0xcc
  10 | 0xab
  11 | 0x20
  12 | 0x00 - log entry type LOG_ENTRY_TYPE_STATE
  13 | 0x2a - entry term 42
  14 | 0x00
  15 | 0x00
  16 | 0x00
  17 | 0x00
  18 | 0x00
  19 | 0x00
  20 | 0x66 - log data "foo"
  14 | 0x6f
  15 | 0x6f

zmq frame body as a log entry: [000000000000000000000000, LOG_ENTRY_TYPE_CHECKPOINT, 43, 0xc0]

offs.| value
   0 | 0x00 - 12 byte request id
   1 | 0x00
   2 | 0x00
   3 | 0x00
   4 | 0x00
   5 | 0x00
   6 | 0x00
   7 | 0x00
   8 | 0x00
   9 | 0x00
  10 | 0x00
  11 | 0x00
  12 | 0x02 - log entry type LOG_ENTRY_TYPE_CHECKPOINT
  13 | 0x2b - entry term 43
  14 | 0x00
  15 | 0x00
  16 | 0x00
  17 | 0x00
  18 | 0x00
  19 | 0x00
  20 | 0xc0 - log data 0xc0
```

Peer
====

Each ØMQ RAFT peer opens a single ØMQ ROUTER socket and binds it to the indicated in the configuration ip address and port.

Each ØMQ RAFT peer must have a unique PEER ID (`string`) and an URL of its ROUTER socket.
Information about each peer's ID and a (visible by the cluster) URL must be available to all other peers in the cluster.

Each ØMQ peer opens as many ØMQ DEALER sockets as there are other peers in the cluster, connected to the other peers' ROUTER sockets.

For communication between peers in ØMQ RAFT cluster the following RPC messages are being used:

* RequestVote RPC
* AppendEntries RPC
* InstallSnapshot RPC

Each peer expects a response to its RPC message to be received within a `RPC_TIMEOUT` (preferred 50 ms) interval. When the response is not being received within this interval the requesting peer repeats sending the last message.

For message types: `AppendEntries` and `InstallSnapshot` the `RPC_TIMEOUT` may be longer but not longer than the half of the ELECTION_TIMEOUT_MIN (preferred 200 ms).

The first frame of each of the above message types identifies messages for debouncing. If two messages are being received with the same request id, the second one is being discarded.
The type of the identification frame is `uint`. Its value starts at 1 and increases monotonically for each subsequent message until value reaches 16777216 at which it becomes 0.


Client
======

The ØMQ Raft client should establish a ØMQ DEALER socket and connect it at first to all of the known cluster (seed) peer's ROUTER sockets. Client should then ask each known peer at a time about the cluster state, peer URLs and the current LEADER ID using a RequestConfig RPC. In case the cluster is in the transitional state (no leader) the client should repeat sending RequestConfig messages until the response contains a valid LEADER ID. Upon success the client should disconnect its DEALER socket from all other peers and leave a connection only to the leader's ROUTER URL. The client should also update its list of all known peers in the cluter.

The client may ask for the current cluster status and configuration periodically using RequestConfig RPC to detect changes eagerly.

Once the client knows the current leader it should send all messages to the current leader's url.

The known leader state may be broken if:

1. The client receives negative response from a peer with optional information about the currently elected LEADER ID.
2. The client waits for the response from a peer longer than SERVER_RESPONSE_TTL (preferred: 500 ms)
or the peer responds negatively without indicating the current LEADER ID.

The "broken known leader state" recovery procedure:

In the first scenario the client should connect its DEALER socket to the new leader's URL indicated in the last response, disconnect from other peers and send the last request message again to the new leader.

In the second scenario the client should connect its DEALER sockets to all known cluster peers, wait a SERVER_ELECTION_GRACE_MS (preferred: 300 ms) interval and send the last request message again to each peer in turn until a peer responds with a positive response or a negative response with the information about the currently elected LEADER ID. In this instance the client should connect its DEALER socket to the current leader's URL and disconnect from all other peers and send the last request message again to the new leader.

The client may use the following RPC messages for communicating with the cluster peers:

* RequestConfig RPC - for retrieving current cluster's peer configuration,
* RequestUpdate RPC - for updating the state, that is appending new log entries,
* ConfigUpdate RPC - for changing the cluster's peer configuration,
* RequestEntries RPC - for retrieving snaphost and log entries,
* RequestLogInfo RPC - for retrieving additional information about log entries state,
* any other message following ClientDispatch RPC is being forwarded to the state machine


RequestVote RPC
---------------

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |   uint |       | message id
  2 |  bytes |  0x3f | message type
  3 |  bytes |       | cluster ident
  4 | string |       | candidate's PEER ID
  5 |   uint |       | candidate's current TERM
  6 |   uint |       | candidate's last LOG INDEX
  7 |   uint |       | candidate's last LOG TERM
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |   uint |       | request message id
  2 |   uint |       | responder's current TERM for candidate to update itself
  3 |   bool |       | vote granted
```


AppendEntries RPC
-----------------

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |   uint |       | message id
  2 |  bytes |  0x2b | message type
  3 |  bytes |       | cluster ident
  4 | string |       | leader's PEER ID
  5 |   uint |       | leader's current TERM
  6 |   uint |       | previous LOG INDEX immediately preceding new entries
  7 |   uint |       | LOG TERM of previous LOG INDEX
  8 |   uint |       | leader's COMMIT INDEX

optional:
  9 |  entry |       | optional next log entry
  N |  entry |       | optional next + (N - 9) log entry
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |   uint |       | request message id
  2 |   uint |       | responder's current TERM for leader to update itself
  3 |   bool |       | success - true if log succesfully updated

optional:
  4 |   uint |       | updating log conflict TERM
  5 |   uint |       | updating log conflict LOG INDEX
```


InstallSnapshot RPC
-------------------

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |   uint |       | message id
  2 |  bytes |  0x24 | message type
  3 |  bytes |       | cluster ident
  4 | string |       | leader's PEER ID
  5 |   uint |       | leader's current TERM
  6 |   uint |       | snapshot last included LOG INDEX
  7 |   uint |       | snapshot last included LOG TERM
  8 |   uint |       | byte offset where chunk is positioned in the snapshot file
  9 |   uint |       | total snapshot size
 10 |  bytes |       | snapshot chunk
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |   uint |       | request message id
  2 |   uint |       | responder's current TERM for leader to update itself

optional:
  3 |   uint |       | indicate a byte offset to start sending a next snapshot chunk from
```

ClientDispatch RPC
------------------

This message provides a common signature of any other client RPC messages.

For a custom state-machine implementations the message type should consist of at least 2 bytes.
All single byte message types are reserved for the future extension of ØMQ RAFT protocol.

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  bytes |       | request id (a 12-byte unique reqid or uint32)
  2 |  bytes |       | message type
  3 |  bytes |       | cluster ident
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  bytes |       | request id
```


RequestConfig RPC
-----------------

Use this message to request current cluster configuration. The reponse to this request is always positive,
regardles of the RAFT state of the peer the response is being received from.

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 | uint32 |       | request id
  2 |  bytes |  0x5e | message type
  3 |  bytes |       | cluster ident
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 | uint32 |       | request id
  2 |   bool |       | is the responding peer a leader
  3 |   json |       | LEADER ID as a JSON string or null

optional:
  4 |   json |       | cluster configuration as a JSON array of tuples containing PEER ID and PEER URL as an array of strings: [[PEER1_ID, PEER1_URL], ..., [PEERn_ID, PEERn_URL]]
```


RequestUpdate RPC
-----------------

To make this RPC idempotent, the unique request id used in this message is written as a part of the log entry.
This RPC also defines the time during which the `reqid` is fresh. If the `RequestUpdate RPC` doesn't succeed within that time (due to the cluster down time or a network outage) it will be rejected if finally reaches the cluster.

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |  bytes |  0x3d | message type
  3 |  bytes |       | cluster ident
  4 |  bytes |       | an arbitrary state log data
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |   bool |       | update status

optional:
  3 |   json |       | upon status is true (a success) a committed LOG INDEX as a JSON number
                     | upon status is false (a failure) a LEADER ID as a JSON string or null
```

Once the response's update status is `false`, but the 3rd frame is included, the client should follow the "broken known leader state" recovery procedure.

Once the response's update status is `false` and the 3rd frame is missing, response indicates that a request has expired: a `reqid` is older than REQUEST_UPDATE_TTL (default: 8 hours). This response is final and in this instance the client should indicate a failure to the requesting party.

If the update status is `true` but there is no 3rd frame in the response it indicates that the update was accepted but the log entry hasn't been committed yet. The response message in this form may be sent several times by the server peer and each time it's being received, the client should reset its RPC timeout clock.

Once the response's update status is `true` and the LOG INDEX is being included as a 3rd frame, response indicates a successfull update. This response is final and in this instance the client should indicate a success to the requesting party.


ConfigUpdate RPC
----------------

To make this RPC idempotent, the unique request id used in this message is written as a part of the log entry.
This RPC also defines the time during which the `reqid` is fresh. If the RequestConfigUpdate RPC doesn't succeed within that time (due to the cluster down time or a network outage) it will be rejected if finally reaches the cluster.

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |  bytes |  0x26 | message type
  3 |  bytes |       | cluster ident
  4 |   json |       | a JSON array with the new complete cluster configuration [[PEER_ID, PEER_URL], ...]
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |   uint |       | update status, one of:
                     | 0: not a leader,
                     | 1: accept or success,
                     | 2: configuration argument error,
                     | 3: cluster busy,
                     | 4: expired request id
optional:
  3 |   json |       | in case of status=0 a LEADER ID as a JSON string or null
                     | in case of status=1 and success a committed LOG INDEX as a JSON number
                     | in case of status=2 an error message and type as a JSON object
```

If the new configuration data format is wrong or there are conflicts of PEER_ID-PEER_URL pairs in the new config with the old config (e.g. PEER_ID has different PEER_URL or two PEER_IDs share the same PEER_URL) the status = 2 is returned in the response. The last frame will contain the json object with the "name" and "message" properties as strings. E.g.:

```
{name: "TypeError", message: "peers must be an array"}
```

If the cluster configuration is currently transitional (e.g. the previous configuration update is still in progress), the response's status will be 3. In this instance the client should repeat its request after some reasonable time interval (e.g. a few seconds).

The response's status = 4 indicates that a request has expired: a `reqid` is older than REQUEST_UPDATE_TTL (default: 8 hours). This response is final and in this instance the client should indicate a failure to the requesting party.

Once the response's status is 0 the client should follow the "broken known leader state" recovery procedure.

If the update status=1 but there is no 3rd frame in the response it indicates that the configuration update was accepted, but the log's `Cold,new` entry hasn't been committed yet. The response message in this form may be sent several times by the server peer and each time it's being received, the client should reset its RPC timeout clock.

Once the response's update status=1 and the LOG INDEX is being included as a 3rd frame, response indicates a successfull configuration update. This response is final and in this instance the client should indicate a success to the requesting party.


RequestEntries RPC
------------------

Use this message to read the log entries or a snapshot file content.

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 | uint32 |       | request id
  2 |  bytes |  0x3c | message type
  3 |  bytes |       | cluster ident
  4 |   uint |       | previous LOG INDEX

optional:
  4 |  nuint |       | a number of the requested log entries; in follow up messages 0 may be used to stop server from sending any further responses; `null` to read the log entries up to the commit index
optional:
  5 |   uint |       | next snapshot chunk byte offset, ignored if "previous LOG INDEX" is not before the log's first index.
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 | uint32 |       | request id
  2 |   uint |       | request status, one of: 
                     | 0: not a leader,
                     | 1: this is the last entry,
                     | 2: more entries are being sent,
                     | 3: a snapshot chunk,
  3 |   json |       | in case of status=0 a LEADER ID as a JSON string or null;
                     | in case of status=1 or 2 this frame must be ignored;
                     | in case of status=3 a JSON array containing numbers: [chunk byte offset, snaphost total byte size, snapshot term];

optional:
  4 |   uint |       | a LOG INDEX of the last entry/snapshot chunk sent in this message or a previous LOG INDEX if there are no entries in this response
optional:
  5 |  bytes |       | in case of status=3 the snapshot chunk
  5 |  entry |       | in case of status=1 or 2 the first log entry following "previous LOG INDEX"
  N |  entry |       | in case of status=1 or 2 the last log entry
```

RequestEntries RPC responses are being sent in a streaming pipeline. The peer will respond with up to REQUEST_ENTRIES_PIPELINES (default: 5) response messages prior to expecting the first follow up client request message.

The client upon receiving a response with status=2 or status=3 with the last snapshot chunk should send a follow up request message replacing previous LOG INDEX frame with the value of the LOG INDEX from the response message (no. 4), retaining the "request id" frame value of the previous request.

The client upon receiving a response with status=3 and the response does not contain the last snapshot chunk, should send a follow up request message retaining the "previous LOG INDEX" frame (no. 4) and the "request id" frame value of the previous request. The client may optionally add an expected "next snapshot chunk byte offset" (no. 5) for extra validation.

The client upon receiving a response with status=0 should follow the "broken known leader state" recovery procedure.

The client upon receiving a response with status=1 should end an RPC with a success.

Before receiving the last response with a status=1, the client may stop the server peer from sending further responses (allowing server to clean up the reserved pipeline sooner) by including frame no. 4 with a 0 in the follow up request message. Without it, the pipeline is being cleaned up after REQUEST_ENTRIES_TTL (default: 5-7 seconds). There is a hard limit of request entries RPCs being served in parallel, indicated by a: REQUEST_ENTRIES_HIGH_WATERMARK constant (8000). If the number of concurrent request entries is reached, new requests will be silently ignored by the peer until the number of the requests being served will drop below the watermark.


RequestLogInfo RPC
------------------

Use this message to request information about the peer's individual log state. The reponse to this request is always positive, regardles of the RAFT state of the peer the response is being received from.

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 | uint32 |       | request id
  2 |  bytes |  0x25 | message type
  3 |  bytes |       | cluster ident
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 | uint32 |       | request id
  2 |   bool |       | is the responding peer a leader
  3 |   json |       | LEADER ID as a JSON string or null
  4 |   uint |       | peer's current TERM
  5 |   uint |       | peer's log first INDEX
  6 |   uint |       | peer's state machine's LAST APPLIED INDEX
  7 |   uint |       | peer's COMMIT INDEX
  8 |   uint |       | peer's log last INDEX
  9 |   uint |       | peer's snapshot byte size
 10 |   uint |       | peer's PRUNE INDEX
```


RequestBroadcastStateUrl RPC
----------------------------

This RPC is being handled by the `BroadcastStateMachine` and is only valid when the ØMQ RAFT cluster state machine is a `BroadcastStateMachine`. Use it to retrieve the broadcast state machine's ØMQ PUB socket URL.

```
no. |   type | value | description
-----------------------------------------
  1 | uint32 |       | request id
  2 |  bytes |  0x2a | message type
  3 |  bytes |       | cluster ident
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 | uint32 |       | request id

optional:
  2 | string |       | a broadcast state ØMQ PUB socket url upon success
```

The client, upon missing the 2nd frame in the response, should perform a `RequestConfig RPC` and only when the new leader is established should send this RPC again to the new leader's ROUTER url.

The client, upon receiving ØMQ PUB url may connect a ØMQ SUB socket to this url to start receiving `StateBroadcast` messages.


StateBroadcast MSG
------------------

These messages are being fan out by the `BroadcastStateMachine`'s ØMQ PUB socket.

```
no. |   type | value | description
-----------------------------------------
  1 |  bytes |       | cluster ident
  2 |   uint |       | leader's current TERM
  3 |   uint |       | LAST APPLIED INDEX

optional:
  4 |  entry |       | the first entry included in this broadcast
  N |  entry |       | the last applied entry included in this broadcast
```

The `BroadcastStateMachine` should send an empty StateBroadcast message (without entries) every CLIENT_HEARTBEAT_INTERVAL (preferred: 500 ms) interval or immediately when the new entries has been applied to the state machine.
