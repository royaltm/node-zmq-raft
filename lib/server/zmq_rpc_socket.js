/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";
const isArray = Array.isArray
    , isBuffer = Buffer.isBuffer
    , identity = (a) => a;

const assert = require('assert');
const zmq = require('zeromq');
const { ZMQ_LINGER, ZMQ_SNDHWM } = zmq;
const { ZmqDealerSocket } = require('../utils/zmqsocket');

const DEFAULT_RPC_TIMEOUT = 50;

const { allocBufUIntLE: encodeRequestId, readBufUIntLE: decodeRequestId }  = require('../utils/bufconv');

const REQUEST_ID_BYTES = 3;
const REQUEST_ID_MASK = (1 << (REQUEST_ID_BYTES*8)) - 1;
const requestIdIsValid = (id) => {
  const len = id.length;
  return len > 0 && len <= REQUEST_ID_BYTES;
}

const handler$       = Symbol.for('handler');
const pending$       = Symbol.for('pending');
const connected$     = Symbol.for('connected');
const sockopts$      = Symbol.for('sockopts');
const lastReqId$     = Symbol.for('lastReqId');
const nextRequestId$ = Symbol.for('nextRequestId');
const disconnect$    = Symbol.for('disconnect');

const debug = require('debug')('zmq-raft:rpc-socket');

function RpcCancelError(message) {
  Error.captureStackTrace(this, RpcCancelError);
  this.name = 'RpcCancelError';
  this.message = message || 'request cancelled';
}

RpcCancelError.prototype = Object.create(Error.prototype);
RpcCancelError.prototype.constructor = RpcCancelError;
RpcCancelError.prototype.isCancel = true;

/*
  ZmqRpcSocket is a handy wrapper for zmq DEALER socket that implements RPC pattern.

  ZmqRpcSocket waits forever for reply re-sending request every `timeout` milliseconds if needed.

  ZmqRpcSocket guarantees that only one unique request may be sent at a time (the request may be repeated though).

  Pending requests may be canceled any time.

  The response is handled using promises.

  example:

  rpc = new ZmqRpcSocket('tcp://127.0.0.1:1234', {timeout: 100});
  rpc.request('foo')
  // handle response
  .then(resp => console.log(resp))
  // handle timeout or close error
  .catch(err => console.error(err));

  To cancel pending request invoke reset(), e.g.:

  rpc.reset().request('another request')

*/


class ZmqRpcSocket {

  /**
   * Create ZmqRpcSocket
   *
   * `options` may be one of:
   *
   * - `timeout` {number}: default repeat request timeout in milliseconds
   * - `sockopts` {Object}: specify zmq socket options as object e.g.: {ZMQ_IPV4ONLY: true}
   *
   * @param {string} url
   * @param {Object} options
   * @return {ZmqRpcSocket}
  **/
  constructor(url, options) {
    options || (options = {});
    var sockopts = options.sockopts || {};

    if ('object' !== typeof sockopts)
      throw TypeError('ZmqRpcSocket: sockopts must be an object');

    if ('string' !== typeof url)
      throw TypeError('ZmqRpcSocket: url must be a string');

    this.url = url;
    this.options = Object.assign({}, options);

    this[sockopts$] = Object.keys(sockopts).filter(opt => sockopts.hasOwnProperty(opt))
      .reduce((map, opt) => {
        map.set(toZmqOpt(opt), sockopts[opt]);
        return map;
      }, new Map());

    this.timeoutMs = (options.timeout|0) || DEFAULT_RPC_TIMEOUT;

    if (this.timeoutMs <= 0)
      throw TypeError('ZmqRpcSocket: timeout must be > 0');

    this.socket = null;

    this[connected$] = false;
    this[pending$] = null;
    this[handler$] = null;
    this[lastReqId$] = 0;
  }

  toString() {
    return this.url;
  }

  /**
   * @property pending {Promise|null}
  **/
  get pending() {
    return this[pending$];
  }

  /**
   * @property connected {boolean}
  **/
  get connected() {
    return this[connected$];
  }

  /**
   * Send request
   *
   * @param {Array|primitive} req
   * @param {number} [timeoutMs]
   * @return {Promise}
  **/
  request(req, timeoutMs) {
    if (this[pending$]) return Promise.reject(new Error('ZmqRpcSocket: another request pending'));
    return (this[pending$] = new Promise((resolve, reject) => {
      if (!this[connected$]) this.connect();

      const socket = this.socket;
      const requestId = this[nextRequestId$]();

      var handler = this[handler$] = {requestId: requestId};

      const payload = [encodeRequestId(requestId)].concat(req);

      if (timeoutMs === undefined) timeoutMs = this.timeoutMs;

      var drain, timeout;

      const cleanup = () => {
        if (timeout !== undefined) clearTimeout(timeout);
        if (drain !== undefined) {
          socket.cancelSend();
          socket.removeListener('drain', drain);
        }
        this[handler$] = handler = null;
      }

      const send = () => {
        var sentok = socket.send(payload);
        /* send may invoke 'frames' or 'error' event, which might clear this request */
        if (!handler) {
          if (!sentok) socket.cancelSend();
          debug("rpc.request already gone: %s", this);
        }
        else if (sentok) {
          timeout = setTimeout(send, timeoutMs);
        }
        else {
          timeout = undefined;
          debug("rpc.request queue full: %s", this);
          drain = () => {
            drain = undefined;
            debug("rpc.request drain: %s", this);
            socket.cancelSend();
            timeout = setTimeout(send, timeoutMs);
          }
          socket.once('drain', drain);
        }
      };

      handler.reject = (err) => {
        cleanup();
        reject(err);
      };

      handler.resolve = (arg) => {
        cleanup();
        resolve(arg);
      };

      send();
    }));
  }

  /**
   * Reset socket for another request, optionally disconnecting socket and rejecting pending requests
   *
   * @return {ZmqRpcSocket}
  **/
  reset() {
    if (this[pending$]) this[disconnect$]();
    return this;
  }

  /**
   * Disconnect, close socket and reject pending request
   *
   * @return {ZmqRpcSocket}
  **/
  close() {
    this[disconnect$]();
    var socket = this.socket;
    if (socket) {
      debug("rpc.close: %s", this);
      socket.close();
      this.socket = null;
    }
    return this;
  }

  /**
   * Disconnect, close socket, reject all pending requests and prevent further ones
  **/
  destroy() {
    debug("rpc.destroy: %s", this);
    this.close();
    this.request = destroyed;
    this.connect = destroyed;
  }

  /**
   * Connect socket
   *
   * Use it only to eagerly connect, request() will ensure connect() is being invoked before sending request.
   *
   * @return {ZmqRpcSocket}
  **/
  connect() {
    if (this[connected$]) return this;
    var socket = this.socket || (this.socket = new ZmqDealerSocket());
    /* makes sure socket is really closed when close() is called */
    socket.setsockopt(ZMQ_LINGER, 0);
    for(let [opt, val] of this[sockopts$]) {
      socket.setsockopt(opt, val);
    }
    /* one request a time */
    socket.setsockopt(ZMQ_SNDHWM, 1);
    var url = this.url;
    debug("rpc.connect: %s", url);
    socket.connect(url);
    this[connected$] = true;
    /* error handler */
    socket.on('error', err => {
      var handler = this[handler$];
      if (handler) handler.reject(err);
    });
    /* frames handler */
    socket.on('frames', (args) => {
      var requestId = args.shift();
      if (!requestId || !requestIdIsValid(requestId)) {
        debug("rpc.recv: invalid request id");
        return; /* ignore */
      }
      requestId = decodeRequestId(requestId);
      /* now get the handler associated with requestId */
      var handler = this[handler$];
      if (!handler || handler.requestId !== requestId) {
        debug("rpc.recv: received requestId: %s doesn't match pending response handler", requestId);
        return;
      }
      /* resolve handler promise */
      handler.resolve(args);
      /* cleanup pending only on success
         on error user must reset() socket */
      this[pending$] = null;
    });
    return this;
  }

  /**
   * Get zmq socket option from the underlaying socket
   *
   * @param {string} opt
   * @return {*}
  **/
  getsockopt(opt) {
    return this[sockopts$].get(toZmqOpt(opt));
  }

  /**
   * Set zmq socket option on the underlaying socket
   *
   * @param {string} opt
   * @param {*} value
   * @return {RpcCancelError}
  **/
  setsockopt(opt, value) {
    opt = toZmqOpt(opt);
    this[sockopts$].set(opt, value);
    if (this.socket) this.socket.setsockopt(opt, value);
    return this;
  }

  [nextRequestId$]() {
    var id = (this[lastReqId$] + 1) & REQUEST_ID_MASK;
    this[lastReqId$] = id;
    return id;
  }

  [disconnect$]() {
    var cancel = new RpcCancelError();
    var handler = this[handler$];
    if (handler) handler.reject(cancel);
    this[pending$] = null;

    if (this[connected$]) {
      let socket = this.socket
        , url = this.url;

      socket.removeAllListeners('error');
      socket.removeAllListeners('frames');
      debug("rpc.disconnect: %s", url);
      socket.disconnect(url);
      this[connected$] = false;
    }    
  }

}

function destroyed() {
  throw new Error('ZmqRpcSocket: socket destroyed');
}

function toZmqOpt(opt) {
  var value = ('string' === typeof opt) ? zmq[opt] : opt;
  if ('number' !== typeof value || !isFinite(value)) {
    throw TypeError(`ZmqRpcSocket: invalid socket option: ${opt}`);
  }
  return value;
}

ZmqRpcSocket.encodeRequestId = encodeRequestId;
ZmqRpcSocket.decodeRequestId = decodeRequestId;
ZmqRpcSocket.requestIdIsValid = requestIdIsValid;
ZmqRpcSocket.prototype.encodeRequestId = encodeRequestId;
ZmqRpcSocket.prototype.decodeRequestId = decodeRequestId;
ZmqRpcSocket.prototype.requestIdIsValid = requestIdIsValid;
ZmqRpcSocket.ZmqRpcSocket = ZmqRpcSocket;
ZmqRpcSocket.RpcCancelError = RpcCancelError;
module.exports = exports = ZmqRpcSocket;
