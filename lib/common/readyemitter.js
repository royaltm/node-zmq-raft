/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";
/*

ReadyEmitter class implements common methods:

ready(): Promise - returns promise which resolves when class has finished loading resources
error(err) - emits (fatal) 'error' event and changes class state to unusable (ready() will reject with err from now on)
isReady: bool - returns true if was ever ready (even when later error was called)
getError: Error|null - returns Error instance if error(err) was invoked, otherwise returns null

Implementations should call this[Symbol.for('setReady')]() whenever the asynchronous initiation comletes successfully.
Implementations should call this.error(err) whenever there was an error during initiation
or later a fatal error occured that made this instance unusable.

*/
const EventEmitter = require('events');

const ready$        = Symbol('ready')
    , error$        = Symbol('error')
    , readyPromise$ = Symbol('readyPromise')
    , setReady$     = Symbol.for('setReady')

class ReadyEmitter extends EventEmitter {
  constructor() {
    super();
    this[ready$] = false;
    this[error$] = null;
  }

  /**
   * invoke an error event and mark this instance as erroneous
   *
   * @param {Error} err
  **/
  error(err) {
    this[error$] = err;
    this.emit('error', err);
  }

  /**
   * invoke a ready event and mark this instance as being ready
  **/
  [setReady$]() {
    if (!this[ready$]) {
      this[ready$] = true;
      this.emit('ready', this);
    }
  }

  /**
   * returns a last error
   *
   * @return {null|Error}
  **/
  get getError() {
    return this[error$];
  }

  /**
   * returns an instance ready state
   *
   * @return {boolean}
  **/
  get isReady() {
    return this[ready$];
  }

  /**
   * returns a promise which resolves when the instance becomes ready or rejects with an error
   *
   * @return {Promise}
  **/
  ready() {
    var error = this[error$]
      , readyPromise = this[readyPromise$];

    if (error) return Promise.reject(error);
    else if (readyPromise) return readyPromise;
    else if (this[ready$]) {
      return (this[readyPromise$] = Promise.resolve(this));
    }
    else return (this[readyPromise$] = new Promise((resolve, reject) => {
      var ready = (res) => {
        this.removeListener('error', error);
        resolve(res);
      };
      var error = (err) => {
        this.removeListener('ready', ready);
        reject(err);
      };
      this.once('ready', ready)
          .once('error', error);
    }));
  }
}

module.exports = exports = ReadyEmitter;
