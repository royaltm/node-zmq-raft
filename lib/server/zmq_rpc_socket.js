/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";
const isArray = Array.isArray;
const identity = (a) => a;

const zmq = require('zmq');
const debug = require('debug')('rpc-socket');

const DEFAULT_RPC_TIMEOUT = 50;

const { ZMQ_EVENTS, ZMQ_POLLOUT, ZMQ_LINGER, ZMQ_SNDHWM } = zmq;

const { allocBufUIntLE: encodeRequestId
      , readBufUIntLE:  decodeRequestId }  = require('../utils/bufconv');

const handlers$      = Symbol.for('handlers');
const pending$       = Symbol.for('pending');
const connected$     = Symbol.for('connected');
const sockopts$      = Symbol.for('sockopts');
const lastReqId$     = Symbol.for('lastReqId');
const nextRequestId$ = Symbol.for('nextRequestId');
const disconnect$    = Symbol.for('disconnect');

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
    this[handlers$] = new Map();
    this[lastReqId$] = 0;
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
   * @param {number} [timeout]
   * @return {Promise}
  **/
  request(req, timeout) {
    if (this[pending$]) return Promise.reject(new Error('ZmqRpcSocket: another request pending'));
    return (this[pending$] = new Promise((resolve, reject) => {
      if (!this[connected$]) this.connect();

      const socket = this.socket;
  
      const requestId = this[nextRequestId$]();
      const handler = {resolve: resolve, reject: reject};
      this[handlers$].set(requestId, handler);

      const payload = [encodeRequestId(requestId)].concat(req);

      const send = () => {
        socket.send(payload, 0, err => {
          if (err) {
            clearInterval(handler.interval);
            this[handlers$].delete(requestId);
            reject(err);
          }
        });
      };

      if (timeout === undefined) timeout = this.timeoutMs;

      handler.interval = setInterval(() => {
        if ((socket.getsockopt(ZMQ_EVENTS) & ZMQ_POLLOUT) !== 0) {
          /* only if payload unsent should we push another request */
          debug("re-sending request: %s", this.url);
          send();
        }
      }, timeout);

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
      debug("socket.close: %s", this.url);
      socket.close();
      this.socket = null;
    }
    return this;
  }

  /**
   * Disconnect, close socket, reject all pending requests and prevent further ones
  **/
  destroy() {
    debug("socket.destroy: %s", this.url);
    this.close();
    this.request = destroyed();
    this.connect = destroyed();
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
    var socket = this.socket || (this.socket = zmq.socket('dealer'));
    /* makes sure socket is really closed when close() is called */
    socket.setsockopt(ZMQ_LINGER, 0);
    for(let [opt, val] of this[sockopts$]) {
      socket.setsockopt(opt, val);
    }
    /* one request a time */
    socket.setsockopt(ZMQ_SNDHWM, 1);
    var url = this.url;
    debug("socket.connect: %s", url);
    socket.connect(url);
    this[connected$] = true;

    socket.on('message', (requestId, ...args) => {
      requestId = decodeRequestId(requestId);
      /* now get the handler associated with requestId */
      var handler = this[handlers$].get(requestId);
      if (!handler) {
        debug("socket.recv: received requestId: %s doesn't have a correlated response handler", requestId);
        return;
      }
      /* since we received the reply, clear the timeout */
      clearInterval(handler.interval);
      /* remove handler */
      this[handlers$].delete(requestId);
      /* resolve handler promise */
      handler.resolve(args);
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
    var id = (this[lastReqId$] + 1) & 0xffffff;
    this[lastReqId$] = id;
    return id;
  }

  [disconnect$]() {
    var cancel = new RpcCancelError();
    for (let handler of this[handlers$].values()) {
      clearInterval(handler.interval);
      handler.reject(cancel);
    }
    this[handlers$].clear();
    this[pending$] = null;

    if (this[connected$]) {
      let socket = this.socket
        , url = this.url;

      socket.removeAllListeners('message');
      debug("socket.disconnect: %s", url);
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
ZmqRpcSocket.prototype.encodeRequestId = encodeRequestId;
ZmqRpcSocket.prototype.decodeRequestId = decodeRequestId;
ZmqRpcSocket.ZmqRpcSocket = ZmqRpcSocket;
ZmqRpcSocket.RpcCancelError = RpcCancelError;
module.exports = exports = ZmqRpcSocket;
