/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const assert = require('assert');

const ReadyEmitter = require('../common/readyemitter');

/*

Implementations may implement nothing.

There are 2 special events emitted on the state machine from zmq-raft:

On raft state change:

"raft-state": (state, currentTerm)
- `state{symbol}`: peer's current state (see common.constants.FSM_*)
- `currentTerm{number}`: peer's current term

on client request, when its type is unknown to raft:

"client-request": (reply, msgType, msgArgs)
- `reply{Function}`: function which replies messages to the client: reply([arg1, arg2, ...args]),
  for more about `reply` function see `protocol.FramesProtocol.prototype.createRouterMessageListener`
- `msgType{Buffer}`: a message type (any length >= 1)
- `msgArgs{Array<Buffer>}: the rest of the message frames.

Client's rpc requests sent to the raft's ZMQ_ROUTER socket follows Dispatch RPC. The msgType is the 1st frame,
while the msgArgs are frames beginning with 3rd frame.
The 2nd frame is authorization frame and is discarded from event args.

Original ZMQ_ROUTER ident frame and requestId frame are accessible on `reply` function as:

- `reply.ident{Buffer}`
- `reply.requestId{Buffer}`

They allow to detect and bounce message duplicates.

*/

class StateMachineBase extends ReadyEmitter {

  /**
   * Creates new instance
   *
   * Implementations might load current state from persistent storage and read
   * the `lastApplied` value from it.
   *
   * Implementations must call this[Symbol.for('setReady')]() after initialization.
   *
   * @return this
  **/
  constructor() {
    super();
    this.lastApplied = 0;
  }

  /**
   * This property is read by raft to initialize its `lastApplied` and `commitIndex` values on startup.
   *
   * @property lastApplied{Number}
  **/

  /**
   * closes this instance
   *
   * Implementations should close all resources asynchronously and resolve returned promise.
   *
   * @return {Promise}
  **/
  close() {
    return Promise.resolve();
  }

  /**
   * applies log entries to the state machine
   *
   * Returned promise should resolve to lastApplied value after applying provided entries.
   *
   * `entries` - contains buffers, each buffer for each log entry
   * `nextIndex` - raft log index of the first (potential) entry provided in entries
   * `snapshot` - optional snapshot to reset state machine from
   * 
   * NOTE: `nextIndex` must never be less or equal to `lastApplied`
   *       `nextIndex` must never be grater than lastApplied + 1 or snapshot index + 1
   * 
   * Asynchronous updates must be applied in FIFO order:
   * First in (applied) - first resolved.
   *
   * @param {Array} entries
   * @param {nuber} nextIndex
   * @param {nuber} currentTerm
   * @param {SnapshotBase} [snapshot]
   * @return {Promise}
  **/
  applyEntries(entries, nextIndex, currentTerm, snapshot) {
    assert(nextIndex > this.lastApplied);
    assert(snapshot && nextIndex === snapshot.logIndex + 1 || !snapshot && nextIndex === this.lastApplied + 1);
    this.lastApplied = nextIndex + entries.length - 1;
    return Promise.resolve(this.lastApplied);
  }

}

module.exports = exports = StateMachineBase;
