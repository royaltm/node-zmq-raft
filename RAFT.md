ZMQ RAFT protocol
=================

ZMQ RAFT protocol messages follow the ZMQ standard and consist of ZMQ [frames](https://rfc.zeromq.org/spec:15/ZMTP/). http://zguide.zeromq.org/page:all#ZeroMQ-is-Not-a-Neutral-Carrier

Data types
----------

Protocol message frames body may be encoded as one of the following type:

* `bytes` - an array of bytes containing zero or more bytes
* `string` - an utf-8 encoded string of zero or more bytes (no null termination)
* `uint` - an unsigned least significant byte first variable length integer (1 - 8 bytes)
* `bool` - a boolean (true: body must have at least 1 byte and the first byte must not be 0, false: otherwise)
* `json` - a MessagePack encoded JSON data: http://msgpack.org
* `entry` - a log entry
* `reqid` - 12 byte unique request id

The `reqid` 12-byte request id is constructed as follows:

- 4-byte value representing the seconds since the Unix epoch (most significant byte first)
- 3-byte machine identifier
- 2-byte process id, and
- 3-byte counter, starting with a random value.

The `entry` data is constructed as follows:

- 12-bytes `reqid`
- 1-byte log entry type: 0 - state, 1 - config, 2 - checkpoint
- 7-byte log entry term (least significant byte first)
- arbitrary log data

### Examples:

```
zmq frame body as a `string`: "foo"

offs.| value
   0 | 0x66
   1 | 0x6f
   2 | 0x6f

zmq frame body as an `uint`: 255

offs.| value
   0 | 0xff

zmq frame body as an `uint`: 256

offs.| value
   0 | 0x00
   1 | 0x01

zmq frame body as an `uint`: 9007199254740991

offs.| value
   0 | 0xff
   1 | 0xff
   2 | 0xff
   3 | 0xff
   4 | 0xff
   5 | 0xff
   6 | 0x1f

zmq frame body as a `bool`: False

offs.| value
(empty frame)


zmq frame body as a `bool`: True

offs.| value
   0 | 0x01

zmq frame body as an `json`: null

offs.| value
   0 | 0xc0

zmq frame body as an `json`: [42, "foo", false]

offs.| value
   0 | 0x93
   1 | 0x2a
   2 | 0xa3
   3 | 0x66
   4 | 0x6f
   5 | 0x6f
   6 | 0xc2

zmq frame body as a log `entry`: [5956dc8826f27e10dcccab20, LOG_ENTRY_TYPE_STATE, 42, "foo"]

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

zmq frame body as a log `entry`: [0x000000000000000000000000, LOG_ENTRY_TYPE_CHECKPOINT, 43, 0xc0]

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

Each ZMQ peer opens a single ZMQ ROUTER socket and binds it to the indicated in the configuration ip address and port.

Each ZMQ peer must have a unique PEER ID (`string`) and an URL of its ROUTER socket.
Information about each peer's ID and (external) URL must be available to all other peers in the cluster.

Each ZMQ peer opens as many ZMQ DEALER sockets as there are other peers in the cluster, connected to the other peers' ROUTER sockets.

For communication between peers in the RAFT cluster the following RPC messages are being used:

* RequestVote RPC
* AppendEntries RPC
* InstallSnapshot RPC

Peer's RPC expects a response to be received within RPC_TIMEOUT (preferred 50 ms) interval. When the response is not received within this interval the requesting party repeats sending the last request message.

For some message types: AppendEntries, InstallSnapshot the RPC_TIMEOUT may be longer but not longer than the half of the ELECTION_TIMEOUT_MIN (preferred 200 ms).

The first frame of all the above message types identifies each sent message for debouncing.
This frame type is `uint` and consists of 1 to 3 bytes. Its value starts at 1 and increases monotonically for each subsequent message until value reaches 16777216 at which it becomes 0.


Client
======

A client should establish a ZMQ DEALER socket and connect it at first to any number of the known cluster peer's ROUTER sockets. Client should then ask each known peer at a time about the cluster state, peer URLs and the current LEADER ID using a RequestConfig RPC. In case the cluster is in transit state the client should repeat sending RequestConfig messages until the response contains a valid LEADER ID. Upon success the client should disconnect its DEALER socket from all other peers and connect it only (or leave the connection) to the leader's ROUTER URL. The client should also update its list of all known peers in the cluter.

The client may ask for the current cluster status and configuration periodically using RequestConfig RPC.

The client should send all following messages to the last known elected leader.

There are two conditions that may break the known leader state:

1. The client receives negative response from a peer with optional information about the currently elected LEADER ID.
2. The client waits for the response from a peer longer than SERVER_RESPONSE_TTL (preferred: 500 ms)
or the peer responds negatively without indicating the current LEADER ID.

In the first scenario the client should connect its DEALER socket to the leader's URL indicated in the last response, disconnect from othe peer's and send the last request message again to the new leader.

In the second scenario the client should connect its DEALER sockets to all known cluster peers, wait a SERVER_ELECTION_GRACE_MS (preferred: 300 ms) interval and send the last request message again to each peer in turn until it receives a positive response or a negative response but with the valid information about the currently elected LEADER ID. Upon that the client should connect its DEALER socket to the current leader's URL and disconnect from all other peers.

For the communication between client and peers the following RPC messages are defined:

* RequestConfig RPC
* RequestUpdate RPC
* RequestEntries RPC
* RequestLogInfo RPC
* other, state machine related RPC


RequestVote RPC
---------------

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |   uint |       | message id (1 to 3 bytes)
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
  1 |   uint |       | message id (1 to 3 bytes)
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
  1 |   uint |       | message id (1 to 3 bytes)
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


RequestConfig RPC
-----------------

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |  bytes |  0x5e | message type
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |   bool |       | is the responding peer a leader
  3 |   json |       | LEADER ID as a JSON string or null

optional:
  4 |   json |       | cluster config as a JSON array of tuples containing PEER ID and PEER URL as an array of strings: [[PEER1_ID, PEER1_URL], ..., [PEERn_ID, PEERn_URL]]
```


RequestUpdate RPC
-----------------

To make this RPC idempotent, the unique request id used in this message is written as a part of the log entry.
This RPC also defines the time during which the `reqid` is fresh. If the RequestUpdate RPC doesn't succeed within that time (due to the cluster down time or network outage) it will be rejected if finally reaches the cluster.

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |  bytes |  0x3d | message type
  3 |  bytes |       | an arbitrary state log data
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |   bool |       | update status

optional:
  3 |   json |       | upon status is true (a success) a committed LOG INDEX as a JSON number
                     | upon status is false (a  failure) a LEADER ID as a JSON string or null
```

If the update status is `true` but there is no 3rd frame in the response it indicates that the update was accepted but the log entry hasn't been committed yet. The response message in this form may be sent several times by the peer and each time it is received by the client it should reset the client's RPC timeout clock.

Once the response's update status is `true` and the LOG INDEX is being included as a 3rd frame, response indicates a successfull update.

Once the response's update status is `false` and the 3rd frame is missing, response indicates that a request has expired (a `reqid` is older than REQUEST_UPDATE_TTL (default: 8 hours).

Once the response's update status is `false` but the 3rd frame is included the client should follow the broken leader state procedure.


RequestEntries RPC
------------------

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |  bytes |  0x3d | message type
  3 |   uint |       | previous LOG INDEX

optional:
  4 |   uint |       | number of the requested log entries, may be 0 to stop server from sending any further responses in follow up messages
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |   uint |       | status 0: not a leader, 1: this is the last entry, 2: more entries coming, 3: snapshot chunk
  3 |   json |       | in case of status=0 a LEADER ID as a JSON string or null
                     | in case of status=1 or 2 this frame must be ignored
                     | in case of status=3 a JSON array containing numbers: [chunk byte offset, snaphost total byte size]

optional:
  4 |   uint |       | a LOG INDEX of the last entry/snapshot chunk sent or previous LOG INDEX if there are no entries in the response
optional:
  5 |  bytes |       | in case of status=3 the snapshot chunk
  5 |  entry |       | in case of status=1 or 2 the first log entry following previous LOG INDEX
  N |  entry |       | in case of status=1 or 2 the last log entry
```

RequestEntries RPC responses are being sent in a pipeline. The peer will respond with up to REQUEST_ENTRIES_PIPELINES (default: 5) response messages prior to receiving the follow up client request messages.

The client upon receiving a response with status=2 or 3 should send a follow up request message replacing previous LOG INDEX frame with the value of the LOG INDEX from the response message, retaining unique request id frame value of the previous request.

The client upon receiving response with status=1 should end RPC with a success.

The client upon receiving response with status=0 should follow the broker leader state procedure.

The client may stop the peer from sending further responses (and clean up the reserved pipeline) by including 4th frame equal to 0 in the follow up request message.


RequestLogInfo RPC
------------------

Request frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |  bytes |  0x25 | message type
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |   bool |       | is the responding peer a leader
  3 |   json |       | LEADER ID as a JSON string or null
  4 |   uint |       | peer's current TERM
  5 |   uint |       | peer's log first INDEX
  6 |   uint |       | peer's state machine LAST APPLIED INDEX
  7 |   uint |       | peer's COMMIT INDEX
  8 |   uint |       | peer's log last INDEX
  9 |   uint |       | peer's snapshot byte size
```


RequestBroadcastStateUrl RPC
----------------------------

This RPC is only valid when the ZMQ RAFT cluster state machine is a `BroadcastStateMachine`.

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id
  2 |  bytes |  0x2a | message type
```

Response frames:

```
no. |   type | value | description
-----------------------------------------
  1 |  reqid |       | unique request id

optional:
  2 | string |       | a broadcast state ZMQ PUB socket url upon success
```

The client, upon missing 2nd frame in the response, should perform a RequestConfig RPC and only when the leader is established should perform this RPC.
The client, upon receiving ZMQ PUB socket url may connect a ZMQ SUB socket to this url to start receiving StateBroadcast messages.


StateBroadcast MSG
------------------

These messages are being fan out by the `BroadcastStateMachine`'s ZMQ PUB socket.

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
