/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

/*
  ZmqProtocolSocket is a handy wrapper for zmq DEALER socket that implements RPC pattern.

  ZmqProtocolSocket allows to set timeout for every request, so they will be rejected with TimeoutError
  after timeout occures.

  ZmqProtocolSocket guarantees that responses will be correlated with requests.
  ZmqProtocolSocket DOES NOT guarantee that the order of responses will be the same as the order of requests.

  The response is handled using promises.

  example:

  sock = new ZmqProtocolSocket('tcp://127.0.0.1:1234', options);
  sock.request(['foo'], (args, reply, refresh) => {
    console.log('received response');
    if (args.length !== 1) throw new Error("invalid response!");
    if (args[0].equals(done)) {
      reply('send more');
      refresh();
    }
    else {
     return args;
    }
  }, {timeout: xxx, id: 'deadbaca'});
  // handle response
  .then(resp => console.log(resp))
  // handle timeout or close error
  .catch(err => console.error(err));
*/

const isArray = Array.isArray;
const isBuffer = Buffer.isBuffer;

const assert = require('assert');

const zmq = require('zmq')
    , { ZMQ_EVENTS, ZMQ_POLLOUT } = zmq;

const debug = require('debug')('zmq-socket');

const { allocBufUIntLE: encodeRequestId
      , readBufUIntLE:  decodeRequestId }  = require('../utils/bufconv');

const handlers$       = Symbol.for('handlers');
const connected$      = Symbol.for('connected');
const sockopts$       = Symbol.for('sockopts');
const lastReqId$      = Symbol.for('lastReqId');
const nextRequestId$  = Symbol.for('nextRequestId');
const connectUrls$    = Symbol.for('connectUrls');
const send$           = Symbol('send');
const paused$         = Symbol('paused');

function TimeoutError(message) {
  Error.captureStackTrace(this, TimeoutError);
  this.name = 'TimeoutError';
  this.message = message || 'request timeout';
}

TimeoutError.prototype = Object.create(Error.prototype);
TimeoutError.prototype.constructor = TimeoutError;
TimeoutError.prototype.isTimeout = true;

const ZmqBaseSocket = require('../client/zmq_base_socket');

class ZmqProtocolSocket extends ZmqBaseSocket {

  /**
   * Create ZmqProtocolSocket
   *
   * `options` may be one of:
   *
   * - `urls` {string}: urls of the servers to connect to
   * - `timeout` {number}: default timeout in milliseconds
   * - `lazy` {boolean}: specify `true` to connect lazily on first request
   * - `sockopts` {Object}: specify zmq socket options as object e.g.: {ZMQ_IPV4ONLY: true}
   * - `protocol` {Object}: default frames protocol
   * - `highwatermark` {number}: shortcut to specify ZMQ_SNDHWM for a zmq DEALER socket
   *   this affects how many messages are queued per server so if one of the peers
   *   goes down this many messages are possibly lost
   *   (unless the server goes up and responds within the request timeout)
   *   default is 1
   *                 
   *
   * @param {string|Array} [urls] - this overrides urls set in options
   * @param {number|Object} options or default timeout
   * @return {ZmqProtocolSocket}
  **/
  constructor(urls, options) {
    super(urls, options);

    options = this.options;

    this.protocol = options.protocol;
    this.timeoutMs = options.timeout|0;
    if (!this[sockopts$].has(zmq.ZMQ_SNDHWM))
      this[sockopts$].set(zmq.ZMQ_SNDHWM, (options.highwatermark|0) || 1);
    this[lastReqId$] = 0;
    this[handlers$] = new Map();
    this[paused$] = false;
    if (!options.lazy) this.connect();
  }

  /**
   * Send request
   * `options` may be one of:
   *
   * - `id` {string|Buffer}: custom request id
   * - `timeout` {number}: timeout in milliseconds
   * - `flags` {number}: flags to pass to zmq socket
   * - `protocol` {Object}: frames protocol
   * - `onresponse` {Function}: callback for handling multi-part messages
   *    signature: (msgs, reply, refresh) => result
   *    reply({Array}) allows to send additional requests
   *    refresh([{number}]) allows to refresh timeout
   *    result !== undefined will end request and resolve request promise
   *
   * @param {string|Array} msg
   * @param {Object} [options]
   * @return {Promise}
  **/
  request(msg, options) {
    options || (options = {});

    return new Promise((resolve, reject) => {
      if (!this[connected$]) this.connect();

      var protocol = options.protocol || this.protocol;
      var handler = {protocol: protocol};

      var onresponse = options.onresponse;

      if (onresponse !== undefined) {
        if ('function' !== typeof onresponse) {
          return reject(new TypeError("request: onresponse must be a function"));
        }
        handler.onresponse = onresponse;
      }

      if (protocol !== undefined) msg = protocol.encodeRequest(msg);

      var {key, buf} = this[nextRequestId$](options.id);

      var timeout, timeoutMs;

      handler.resolve = (result) => {
        if (timeout !== undefined) clearTimeout(timeout);
        this[handlers$].delete(key);
        resolve(result);
      };

      var error = handler.reject = (err) => {
        if (timeout !== undefined) clearTimeout(timeout);
        this[handlers$].delete(key);
        reject(err);
      };

      var refresh = handler.refresh = (newTimeoutMs) => {
        if (newTimeoutMs !== undefined) timeoutMs = (newTimeoutMs|0);
        if (timeout !== undefined) clearTimeout(timeout);
        timeout = timeoutMs > 0 ? setTimeout(() => error(new TimeoutError()), timeoutMs)
                                : undefined;
      };

      this[handlers$].set(key, handler);
      var message = [buf].concat(msg);

      refresh((options.timeout|0) || this.timeoutMs);

      this[send$](key, handler, message, options.flags);
      message = buf = options = null;
    });
  }

  /**
   * Disconnect, close socket and reject all pending requests
   *
   * @return {ZmqProtocolSocket}
  **/
  close() {
    super.close();
    // reject all handlers and clear timeouts
    for (var handler of this[handlers$].values()) {
      handler.reject(new Error("closed"));
    }
    assert(this[handlers$].size === 0)
    this[paused$] = false;
    return this;
  }

  /**
   * Connect socket
   *
   * @return {ZmqProtocolSocket}
  **/
  connect() {
    if (this[connected$]) return;
    var socket = this.socket || (this.socket = zmq.socket('dealer'));
    /* makes sure socket is really closed when close() is called */
    socket.setsockopt(zmq.ZMQ_LINGER, 0);
    /* set some socket options */
    for(let [opt, val] of this[sockopts$]) socket.setsockopt(opt, val);
    this[connectUrls$](this.urls);
    this[connected$] = true;
    /* install handler */
    socket.on('message', (id, ...args) => {
      var key = id.length === 12 ? id.toString('hex') : decodeRequestId(id);
      /* get the handler associated with request id */
      var handler = this[handlers$].get(key);
      if (handler === undefined) {
        debug("socket.recv: unexpected request id: %s in response, will drop the response", key);
        return;
      }
      var result;
      var onresponse = handler.onresponse;

      /* optionally decode response */
      var protocol = handler.protocol;
      if (protocol !== undefined) args = protocol.decodeResponse(args);

      if (onresponse !== undefined) {
        /* handle onresponse callback */
        try {
          result = onresponse(args, (msg, flags) => {
            if (protocol !== undefined) msg = protocol.encodeRequest(msg);
            this[send$](key, handler, [id].concat(msg), flags);
          }, handler.refresh);
        } catch(err) {
          handler.reject(err);
        }
      }
      else {
        /* just respond */
        result = args;
      }
      if (result !== undefined) handler.resolve(result);
    });

    return this;
  }

  /**
   * @property paused {boolean}
  **/
  get paused() {
    return this[paused$];
  }

  [send$](key, handler, message, flags) {
    var immediate = false;
    const callback = (err) => {
      /* a hack to detect if msg was sent immediately or queued in node-zmq
         it would be much better if send would return a boolean instead */
      immediate = true;

      if (err) handler.reject(err);

      if (this[paused$]) {
        debug('socket.send: flushing queue');
        flush();
      }
    };
    /* flush queued messages that otherwise will be removed if expired */
    const flush = () => {
      this[paused$] = false;

      for(handler of this[handlers$].values()) {
        let queue = handler.queue;
        if (isArray(queue)) {
          while (queue.length !== 0) {
            let {message, flags} = queue.shift();

            immediate = false;

            this.socket.send(message, flags, callback);

            if (!immediate) {
              debug('socket.send: pushed back while flushing');
              this[paused$] = true;
              return false;
            }
          }
        }
      }
      return true;
    };

    /* if last send wasn't immediate and zmq socket hit hwmark on all server queues try flushing
       if couldn't flush all queued messages then put on hold the current one */
    if (this[paused$] && ((this.socket.getsockopt(ZMQ_EVENTS) & ZMQ_POLLOUT) === 0 || !flush())) {
      let queue = handler.queue || (handler.queue = []);
      queue.push({message: message, flags: flags});
      return;
    }

    this.socket.send(message, flags, callback);

    if (!immediate) {
      debug('socket.send: pushed back');
      this[paused$] = true;
    }

  }

  [nextRequestId$](id) {
    var req = {};
    if (id === undefined) {
      id = (this[lastReqId$] = this[lastReqId$] + 1 >>> 0);
      req.buf = encodeRequestId(id);
      req.key = id;
    }
    else if (isBuffer(id) && id.length === 12) {
      req.buf = id;
      req.key = id.toString('hex');
    }
    else if ('string' !== typeof id || id.length !== 24) {
      throw new TypeError("id must be a 12-byte buffer instance or a hex string");
    }
    else {
      req.buf = Buffer.from(id, 'hex');
      req.key = id;
    }
    return req;
  }

}

ZmqProtocolSocket.ZmqProtocolSocket = ZmqProtocolSocket;
ZmqProtocolSocket.TimeoutError = TimeoutError;
module.exports = exports = ZmqProtocolSocket;
