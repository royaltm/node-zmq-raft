/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";
/*

ReadyEmitter class implements common methods:

ready(): Promise - returns promise which resolves when class has finished loading resources
error(err) - emits (fatal) 'error' event and changes class state to unusable (ready() will reject with err from now on)
isReady: bool - returns true if was ever ready (even when later error was called)
getError: Error|null - returns Error instance if error(err) was invoked, otherwise returns null

*/
const EventEmitter = require('events');

const ready$ = Symbol('ready')
    , error$ = Symbol('error')
    , readyPromise$ = Symbol('readyPromise')
    , setReady$ = Symbol.for('setReady')

class ReadyEmitter extends EventEmitter {
  constructor() {
    super();
    this[ready$] = false;
    this[error$] = null;
  }

  error(err) {
    this[error$] = err;
    this.emit('error', err);
  }

  [setReady$]() {
    if (!this[ready$]) {
      this[ready$] = true;
      this.emit('ready', this);
    }
  }

  get getError() {
    return this[error$];
  }

  get isReady() {
    return this[ready$];
  }

  ready() {
    if (this[error$]) return Promise.reject(this[error$]);
    else if (this[ready$]) return Promise.resolve(this);
    else if (this[readyPromise$]) return this[readyPromise$];
    return this[readyPromise$] = new Promise((resolve, reject) => {
      var ready = (res) => {
        this.removeListener('error', error);
        resolve(res);
      };
      var error = (err) => {
        this.removeListener('ready', ready);
        reject(err);
      };
      this.once('ready', ready);
      this.once('error', error);
    });
  }
}

module.exports = exports = ReadyEmitter;
