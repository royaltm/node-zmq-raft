/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const THRESHOLD_SIZE_NEW_FILE = 512*1024;

const assign              = Object.assign
    , getOwnPropertyNames = Object.getOwnPropertyNames

const path = require('path');
const { createReadStream, createWriteStream } = require('fs');

const { createDecodeStream, Encoder } = require("msgpack-lite");

const { defineConst } = require('../utils/helpers');

const synchronize = require('../utils/synchronize');

const { close, fdatasync, link, openDir, readdir, renameSyncDir, unlink } = require("../utils/fsutil");

const { mixin: mixinHistoryRotation, createRotateName } = require('../utils/filerotate');

const { createTempName, cleanupTempFiles } = require('../utils/tempfiles');

const PersistenceBase = require('../api/persistence_base');

const debug = require('debug')('zmq-raft:file-persistence');

const encoder$        = Symbol('encoder')
    , fd$             = Symbol('fd')
    , writeStream$    = Symbol('writeStream')

const apply$          = Symbol.for('apply')
    , byteSize$       = Symbol.for('byteSize')
    , close$          = Symbol.for('close')
    , init$           = Symbol.for('init')
    , rotate$         = Symbol.for('rotate')
    , update$         = Symbol.for('update')
    , validate$       = Symbol.for('validate')

class FilePersistence extends PersistenceBase {

  /**
   * Creates new instance
   *
   * `filename` should contain path to a filename in some existing directory.
   * `initial` should contain all the state properties set to their intial state.
   *
   * A new file will be created if `filename` does not exist.
   *
   * Implementations must call super(filename, initial) in the constructor.
   * Implementations may implement [Symbol.for('init')]() method for additional asynchronous initialization.
   * Implementations should call this[Symbol.for('setReady')]() upon successfull initialization.
   * Implementations should call this.error(err) upon error while initializing.
   *
   * @param {string} filename
   * @param {Object} initial
   * @return this
  **/
  constructor(filename, initial) {
    super(initial);

    defineConst(this, 'filename', filename);

    this[byteSize$] = 0;

    var onready = (flags, data, byteSize) => {

      const error = (err) => this.error(err);

      const writeStream = createWriteStream(filename, {flags: flags})
      .on('error', error)
      .on('open', fd => {
        writeStream.removeListener('error', error);

        debug('file "%s" opened with flags: %s', filename, flags);

        new Promise((resolve, reject) => {
          this[apply$](this[validate$](data));

          this[byteSize$]    = byteSize;
          this[encoder$]     = new Encoder();
          this[fd$]          = fd;
          this[writeStream$] = writeStream;

          debug('ready: %j', data);

          cleanupTempFiles(filename, debug).catch(err => debug('temporary files cleanup ERROR: %s', err));

          resolve(this[init$]());
        })
        .then(() => this[Symbol.for('setReady')]())
        .catch(err => {
          this[writeStream$] = null;
          this[fd$] = null;
          close(fd);
          this.error(err);
        });
      });
    };

    const read = () => {
      var lastData = assign({}, this.defaultData);
      var byteSize = 0;
      var decodeStream = createDecodeStream();
      var readStream = createReadStream(filename)
      .on('error', err => {
        if (err.code === "ENOENT") {
          debug('no such file: "%s", creating new state', filename);
          setImmediate(onready, 'ax', lastData, 0);
        }
        else this.error(err);
      })
      .on('open', fd => {
        debug('file "%s" opened for reading: %s', filename, fd);
      })
      .on("data", chunk => {
        byteSize += chunk.byteLength;
      });

      readStream.pipe(decodeStream)
      .on("data", data => {
        if (data && 'object' === typeof data && lastData) assign(lastData, data);
        else lastData = undefined;
      })
      .on("end", () => {
        debug('finished reading: %s bytes', byteSize);
        if (decodeStream.decoder.buffer && 
            decodeStream.decoder.buffer.length !== decodeStream.decoder.offset) {
          debug("unfinished chunk detected, a new file will be created on next update");

          byteSize = THRESHOLD_SIZE_NEW_FILE;
        }
        setImmediate(onready, 'a', lastData, byteSize);
      });
    };

    read();
  }

  /**
   * additional initialization
   *
   * Implementations may initialize additional resources asynchronously,
   * in such case this method should return promise.
   *
   * @return {Promise}
  **/
  [init$]() {
    /* no-op */
  }

  /**
   * closes this instance
   *
   * Implementations must call super.close() in overloaded method.
   * However if overloaded method also need to call synchronize on this instance
   * they might call super[Symbol.for('close')]() instead.
   *
   * @return {Promise}
  **/
  close() {
    return synchronize(this, () => this[close$]());
  }

  /**
   * closes storage file
   *
   * Implementations may call this method instead super.close() from synchronize callback:
   *
   * synchronize(this, () => this[Symbol.for('close')]());
   * 
   * @return {Promise}
  **/
  [close$]() {
    var writeStream = this[writeStream$];
    this[writeStream$] = this[fd$] = null;

    return new Promise((resolve, reject) => {
      if (writeStream) {
        debug('closing');
        writeStream.end(resolve);
      }
      else resolve();
    });
  }

  /**
   * update state rotating current file
   *
   * Implementations normally should not overload this method.
   * This method is automatically called from update when size
   * of the current file exceeds THRESHOLD_SIZE_NEW_FILE.
   *
   * @param {Object} properties
   * @return {Promise}
  **/
  rotate(properties) {
    return synchronize(this, () => this[rotate$](properties));
  }

  [rotate$](properties) {
    var _writeStream = this[writeStream$];
    if (!_writeStream) throw new Error("persistent already closed");

    var data = this[validate$](properties, true);

    var filename = this.filename;
    var tmpfilename = createTempName(filename);

    return Promise.all([

      new Promise((resolve, reject) => {
        var encoder = this[encoder$];
        encoder.write(data);
        var chunk = encoder.read();

        this[byteSize$] = 0;

        this[fd$] = null;

        var writeStream = this[writeStream$] = createWriteStream(tmpfilename, {flags: 'ax'})
        .on('error', reject)
        .on('open', fd => this[fd$] = fd);

        writeStream.write(chunk, () => {
          writeStream.removeListener('error', reject);
          this[byteSize$] += chunk.byteLength;
          fdatasync(this[fd$]).then(resolve, reject);
        });
      }),

      new Promise((resolve, reject) => {
        _writeStream.on('error', reject);
        _writeStream.end(resolve);
      })

    ])
    .then(() => link(filename, createRotateName(filename)))
    .then(() => renameSyncDir(tmpfilename, filename))
    .catch(err => {
      this[byteSize$] = THRESHOLD_SIZE_NEW_FILE; // in case of a failure next update will rotate
      throw err;
    })
    .then(() => {
      this[apply$](data);
      this.triggerHistoryRotation();
    });
  }

  /**
   * update state
   *
   * Implementations normally should not overload this method.
   *
   * @param {Object} properties
   * @return {Promise}
  **/
  update(properties) {
    return synchronize(this, () => this[update$](properties));
  }

  /**
   * update state
   *
   * Implementations normally should not overload this method.
   *
   * @param {Object} properties
   * @return {Promise}
  **/
  [update$](data) {
    var writeStream = this[writeStream$];

    if (!writeStream) throw new Error("persistent already closed");

    var byteSize = this[byteSize$];
    if (byteSize >= THRESHOLD_SIZE_NEW_FILE) {
      debug("update: file is large enough to be rotated: %s", byteSize);
      return this[rotate$](data);
    }

    var encoder = this[encoder$];
    data = this[validate$](data)
    encoder.write(data);
    var chunk = encoder.read();

    return new Promise((resolve, reject) => {
      writeStream.on('error', reject);
      writeStream.write(chunk, () => {
        this[byteSize$] += chunk.byteLength;
        writeStream.removeListener('error', reject);
        this[apply$](data);
        fdatasync(this[fd$]).then(resolve, reject);
      });
    }).catch(err => {
      this[byteSize$] = THRESHOLD_SIZE_NEW_FILE; // in case of a failure next update will rotate
      throw err;
    });
  }

  /**
   * apply properties
   *
   * Implementations may customize this method.
   *
   * @param {Object} properties
  **/
  [apply$](properties) {
    getOwnPropertyNames(properties).forEach(name => {
      if (properties[name] !== undefined) this[name] = properties[name];
    });
  }

  /**
   * validate properties
   *
   * Implementations should customize this method which would throw error on invalid value
   * type and remove all properties which values are `undefined`.
   * When `withAllProperties` is true implementations must fill undefined properties
   * with current values.
   *
   * @param {Object} properties
   * @param {bool} withAllProperties
   * @return {Object}
  **/
  [validate$](properties, withAllProperties) {
    var data = {};
    getOwnPropertyNames(properties).forEach(name => {
      if (properties[name] !== undefined) data[name] = properties[name];
    });
    if (withAllProperties) {
      getOwnPropertyNames(this.defaultData).forEach(name => {
        if (data[name] === undefined) data[name] = this[name];
      });
    }
    return data;
  }

}

mixinHistoryRotation(FilePersistence.prototype, debug);

module.exports = FilePersistence;

/*

var msgpack=require('msgpack-lite')
var ben=require('ben')
var obj={alpha:1,beta:'gamma delta epsylon'};
ben(100000, () => msgpack.encode(obj))
var e=new msgpack.Encoder();
ben(100000, () => {e.write(obj); return e.read()})
var FilePersistence = require('./raft/persistence')
var p=new FilePersistence('tmp/persistence.state');p.ready().then(()=>console.log('ok'), console.error)
var ben=require('ben')
ben.async(10000,cb => p.update({currentTerm: p.currentTerm+1}).then(cb,e=>console.error(e)),res=>console.log('\nresult: %s',res))
*/
