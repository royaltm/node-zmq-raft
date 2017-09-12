/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const { Writable } = require('stream');

/**
 * Creates stateMachine stream writer
 *
 * Writer expects each written chunk to be an array of buffers
 * representing log entries starting from index `firstIndex`.
 *
 * @param {StateMachineBase} stateMachine
 * @param {number} firstIndex
 * @param {number} currentTerm
 * @param {SnapshotFile} [snapshot]
 * @return {Promise}
**/
class StateMachineWriter extends Writable {
  constructor(stateMachine, firstIndex, currentTerm, snapshot) {
    super({objectMode: true, highWaterMark: 2});

    this.stateMachine = stateMachine;
    this.currentTerm = currentTerm;

    this.index = firstIndex;

    this._applying = snapshot ? stateMachine.applyEntries([], firstIndex, currentTerm, snapshot)
                                .catch(err => this.emit('error', err))
                              : Promise.resolve();
  }

  _write(entries, encoding, callback) {
    this._applying.then(() => {
      var nextIndex = this.index;
      return this._applying = this.stateMachine.applyEntries(entries, nextIndex, this.currentTerm)
      .then(lastApplied => {
        nextIndex += entries.length;
        this.index = nextIndex;
        if (lastApplied !== nextIndex - 1) throw new Error("applying entries failed");
        callback();
      });
    }).catch(err => callback(err));
  }
}

module.exports = StateMachineWriter;
