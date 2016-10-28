/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

/*

Implementations must implement update(data) method.
Implementations may implement close() method.

*/
const { defineConst } = require('../utils/helpers');

const ReadyEmitter = require('../common/readyemitter');

class PersistenceBase extends ReadyEmitter {

  /**
   * Creates new instance
   *
   * `initial` should contain all the state properties set to their intial state.
   *
   * Implementations must call super(initial) in the constructor.
   * Implementations should read stored properties values from the persistent storage
   * and assign them to this instance.
   * Implementations should call this[Symbol.for('setReady')]() upon successfull initialization.
   * Implementations should call this.error(err) upon error while initializing.
   *
   * @param {Object} initial
   * @return this
  **/
  constructor(initial) {
    super();

    if (!initial || 'object' !== typeof initial) return this.error(new Error("persistence: initial argument must be an object"));

    initial = Object.assign({}, initial);

    defineConst(this, 'defaultData', Object.freeze(initial));

    Object.assign(this, initial);
  }

  /**
   * closes this instance
   *
   * Implementations should close all resources asynchronously and resolve returned promise.
   *
   * @return {Promise}
  **/
  close() {
    /* no-op */
    return Promise.resolve();
  }

  /**
   * update state
   *
   * Implementations should update state asynchronously and resolve returned promise when done.
   * Not all the state properties may be set on `properties` argument.
   * Implementations should retain current properties when argument's property value is undefined.
   *
   * `properties[propertyName] === undefined`.
   *
   * The state must be updated ACID'ly: Atomicly, Consistently, in Isolation and Durably.
   * Asynchronous updates must be applied in FIFO order:
   * First in (updated) - first on "plate" - first resolved.
   * Only after all the updated properties has been transerred to some persistent storage,
   * the implementations must assign new values to this instance.
   *
   * @param {Object} properties
   * @return {Promise}
  **/
  update(properties) {
    throw new Error("persistence.update: implement in subclass");
    /* example
      var data = {};
      Object.getOwnPropertyNames(properties).forEach(name => {
        if (properties[name] !== undefined) data[name] = properties[name];
      });
      return transferToStorageInFifoOrder(data)
      .then(() => Object.assign(this, data));
    */
  }

}

module.exports = exports = PersistenceBase;
