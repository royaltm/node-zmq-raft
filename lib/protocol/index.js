/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const FramesProtocol = require('../protocol/frames_protocol');

exports.FramesProtocol = FramesProtocol;

exports.createFramesProtocol = function(name, options) {
  var [requestSchema, responseSchema, opts] = exports[name + 'RPC'];
  return new FramesProtocol(requestSchema, responseSchema,
                            Object.assign({name: toProtocolName(name)}, options || {}, opts));
}

function toProtocolName(name) {
  return name.toLowerCase();
}

/* common frames for router dispatching */

exports.DispatchRPC = [
  /* request */
  [
    'buffer', // type
    'buffer', // secret
  ],
  /* response */
  [],
  /* options */
  {
    required: [2, 0],
    extraArgs: [true, true]
  }
];

/* RAFT peer messages */

exports.RequestVoteRPC = [
  /* request */
  [
    'buffer', // type
    'buffer', // secret
    'string', // candidateId
    'uint',   // term
    'uint',   // lastLogIndex
    'uint'    // lastLogTerm
  ],
  /* response */
  [
    'uint', // term
    'bool'  // voteGranted
  ],
  /* options */
  {
    required: [6, 2]
  }
];

exports.AppendEntriesRPC = [
  /* request */
  [
    'buffer', // type
    'buffer', // secret
    'string', // leaderId
    'uint',   // leaderTerm
    'uint',   // prevLogIndex
    'uint',   // prevLogTerm
    'uint'    // commitIndex
    // ...log entries
  ],
  /* response */
  [
    'uint', // term
    'bool', // success
    'uint', // [conflictTermIndex]
    'uint'  // [conflictTerm]
  ],
  /* options */
  {
    required: [7, 2],
    extraArgs: [true, false]
  }
];

exports.InstallSnapshotRPC = [
  /* request */
  [
    'buffer', // type
    'buffer', // secret
    'string', // leaderId
    'uint',   // leaderTerm
    'uint',   // lastIncludedIndex - the snapshot replaces all entries up through and including this index
    'uint',   // lastIncludedTerm - term of lastIncludedIndex
    'uint',   // offset - byte offset where chunk is positioned in the snapshot file
    'uint',   // size - total snapshot size
    'buffer'  // data
  ],
  /* response */
  [
    'uint', // term
    'uint'  // [position] will reset sending snapshot from position
  ],
  /* options */
  {
    required: [9, 1]
  }
];

/* client messages */

exports.RequestUpdateRPC = [
  /* request */
  [
    'buffer', // type
    'buffer', // secret
    'buffer'  // log data
  ],
  /* response */
  [
    'bool',   // success
    'object'  // [leaderId|index]
  ],
  /* options */
  {
    required: [3, 1]
  }  
];

exports.ConfigUpdateRPC = [
  /* request */
  [
    'buffer', // type
    'buffer', // secret
    'object'  // new cluster configuration [[PEER_ID, PEER_URL], ...]
  ],
  /* response */
  [
    'uint',   // status: 0 - not a leader, 1 - accept/success, 2 - config error, 3 - busy in transition, 4 - expired
    'object'  // [leaderId|index|error]
  ],
  /* options */
  {
    required: [3, 1]
  }
];

exports.RequestEntriesRPC = [
  /* request */
  [
    'buffer', // type
    'buffer', // secret
    'uint',   // prevIndex
    'nuint',  // [count] 0 means to stop streaming (no response)
    'uint'    // snapshotOffset next
  ],
  /* response */
  [
    'uint',   // status 0: not a leader, 1: this is the last entry 2: there are more 3: snapshot chunk
    'object', // leaderId|[byteOffset, snaphostSize]
    'uint'    // lastIndex of sent entries or a snapshot index
    // ...log entries or a snapshot chunk
  ],
  /* options */
  {
    required: [3, 2],
    extraArgs: [false, true]
  }  
];

exports.RequestConfigRPC = [
  /* request */
  [
    'buffer', // type
    'buffer'  // secret
  ],
  /* response */
  [
    'bool',   // isLeader
    'object', // leaderId
    'object'  // peers
  ],
  /* options */
  {
    required: [2, 3]
  }  
];

exports.RequestLogInfoRPC = [
  /* request */
  [
    'buffer', // type
    'buffer'  // secret
  ],
  /* response */
  [
    'bool',   // isLeader
    'object', // leaderId
    'uint',   // currentTerm
    'uint',   // firstIndex
    'uint',   // lastApplied
    'uint',   // commitIndex
    'uint',   // lastIndex
    'uint',   // snapshotDataSize
    'uint'    // pruneIndex
  ],
  /* options */
  {
    required: [2, 9]
  }  
];

/* broadcast state machine messages */

exports.StateBroadcastRPC = [
  /* request */
  [
    'buffer', // secret
    'uint',   // leader's term
    'uint'    // lastLogIndex including the following log entries
  ],
  /* response */
  [],
  /* options */
  {
    required: [3, 0],
    extraArgs: [true, false]
  }
];
