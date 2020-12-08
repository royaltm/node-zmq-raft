/* 
 *  Copyright (c) 2016-2020 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

/*
  ZmqProtocolSocket is a handy wrapper for zeromq DEALER socket that implements RPC pattern.

  The requests are handled with promises.

  ZmqProtocolSocket allows user to set timeout for every request. Result promise will be rejected
  with TimeoutError on response timeout.

  ZmqProtocolSocket makes best effort that responses will be correlated with requests.
  ZmqProtocolSocket DOES NOT guarantee that the order of responses will be the same as the order of requests.

  Streaming can be realized by multi-reply and multi-request RPCs.
  See the `onresponse` option of the ZmqProtocolSocket.prototype.request method.

  When request promise is being resolved or rejected any stale responses are being discarded.

  Example:

    let sock = new ZmqProtocolSocket('tcp://127.0.0.1:1234', {timeout: 500});

    # simple request
    let response = await sock.request(['foo']);

    # streaming request
    let response = await sock.request(['bar'], {
      onresponse: (resp, reply, refresh) => {
        console.log('received response');
        if (resp.length !== 1) throw new Error("invalid response!");
        if (resp[0].equals(processing)) {
          reply('ok waiting');
          refresh();
        }
        else {
         return resp;
        }
      });
*/

const isArray = Array.isArray;
const isBuffer = Buffer.isBuffer;

const assert = require('assert');

const { ZMQ_LINGER, ZMQ_SNDHWM } = require('zeromq');

const { ZmqDealerSocket } = require('../utils/zmqsocket');

const debug = require('debug')('zmq-raft:socket');

const { allocBufUIntLE: encodeRequestId, readBufUIntLE }  = require('../utils/bufconv');

const decodeRequestId = (id) => {
  if (id === undefined) return;
  const len = id.length;
  if (len === 12) return id.toString('hex');
  else if (len > 0 && len <= 4) return readBufUIntLE(id);
};

const connected$      = Symbol.for('connected');
const connectUrls$    = Symbol.for('connectUrls');
const handlers$       = Symbol.for('handlers');
const queues$         = Symbol.for('queues');
const sockopts$       = Symbol.for('sockopts');
const lastReqId$      = Symbol.for('lastReqId');
const nextRequestId$  = Symbol.for('nextRequestId');
const send$           = Symbol('send');
const flush$          = Symbol('flush');
const isFlushing$     = Symbol('isFlushing');
const deferFlush$     = Symbol('deferFlush');

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
   * `options`:
   *
   * - `urls` {string|Array<string>}: An url or urls of the servers to connect to.
   * - `timeout` {int32}: Default response timeout in milliseconds; 0, negative or not specified
   *                      disables default timeout.
   * - `lazy` {boolean}: Specify `true` to connect lazily on the first request.
   * - `sockopts` {Object}: Specify zeromq socket options as an object e.g.: {ZMQ_IPV4ONLY: true}.
   * - `protocol` {FramesProtocol}: The default frames protocol; by default the raw frames are passed.
   * - `highwatermark` {number}: A shortcut to specify `ZMQ_SNDHWM` socket option for an underlying
   *                   zeromq DEALER socket; this affects how many messages are queued per server
   *                   so if one of the peers goes down this many messages are possibly lost;
   *                   setting it prevents spamming a peer with expired messages when temporary
   *                   network partition occures (default: 2).
   *
   * @param {string|Array} [urls] - overrides urls set in options.
   * @param {number|Object} options or default timeout
   * @return {ZmqProtocolSocket}
  **/
  constructor(urls, options) {
    super(urls, options);

    options = this.options;

    this.protocol = options.protocol;
    this.timeoutMs = options.timeout|0;
    if (!this[sockopts$].has(ZMQ_SNDHWM)) {
      this[sockopts$].set(ZMQ_SNDHWM, (options.highwatermark|0) || 2);
    }
    this[lastReqId$] = 0;
    this[handlers$] = new Map();
    this[queues$] = new Map();
    this[isFlushing$] = false;
    this[deferFlush$] = null;

    if (!options.lazy) this.connect();
  }

  /**
   * Send an RPC request.
   *
   * `options`:
   *
   * - `id` {string|Buffer}: A custom request ID, if not specified a unique int32 request ID
   *                         will be generated.
   * - `timeout` {int32}: A request timeout override in milliseconds;
   *                      if 0 or unspecified uses default timeout;
   *                      negative values disable the timeout completely.
   * - `protocol` {FramesProtocol}: Frames protocol used for the RPC.
   * - `onresponse` {Function}: A callback for handling multi-reply RPCs.
   *
   * The `onresponse` function signature is:
   *    (
   *      responseMessage: Array,
   *      reply: (msg: Array|string|Buffer) => void,
   *      refresh: (int32|undefined) => void
   *    ) => undefined|any
   *
   * - `reply` can be used to send additional requests.
   * - `refresh` can be used to restart timeout counter, optionally overriding the `timeout` interval.
   *
   * If `onresponse` returns something other than `undefined`, the RPC will be finished and the RPC
   * promise resolves to the returned value. Returning `undefined` indicates that more responses
   * are to be expected.
   *
   * @param {string|Array} msg - a request message to send.
   * @param {Object} [options]
   * @return {Promise} resolves to the RPC result.
  **/
  request(msg, options) {
    options || (options = {});

    return new Promise((resolve, reject) => {
      if (!this[connected$]) this.connect();

      const protocol = options.protocol || this.protocol;
      var handler = {protocol: protocol};

      const onresponse = options.onresponse;

      if (onresponse !== undefined) {
        if ('function' !== typeof onresponse) {
          return reject(new TypeError("request: onresponse must be a function"));
        }
        handler.onresponse = onresponse;
      }

      if (protocol !== undefined) msg = protocol.encodeRequest(msg);

      var {key: requestKey, buf: requestBuf} = this[nextRequestId$](options.id);

      var timeout, timeoutMs;

      /* idempotent */
      handler.resolve = (result) => {
        if (timeout !== undefined) clearTimeout(timeout);
        timeout = handler = undefined;
        this[handlers$].delete(requestKey);
        resolve(result);
      };

      /* idempotent */
      const error = handler.reject = (err) => {
        if (timeout !== undefined) clearTimeout(timeout);
        timeout = handler = undefined;
        this[handlers$].delete(requestKey);
        this[queues$].delete(requestKey);
        reject(err);
      };

      const refresh = handler.refresh = (newTimeoutMs) => {
        if (handler === undefined) return;
        if (newTimeoutMs !== undefined) timeoutMs = (newTimeoutMs|0);
        if (timeout !== undefined) clearTimeout(timeout);
        timeout = timeoutMs > 0 ? setTimeout(() => error(new TimeoutError()), timeoutMs)
                                : undefined;
      };

      var payload = [requestBuf].concat(msg);
      this[handlers$].set(requestKey, handler);

      refresh((options.timeout|0) || this.timeoutMs);

      this[send$](requestKey, payload);
      payload = requestBuf = options = null;
    });
  }

  /**
   * Disconnect, close socket and reject all pending requests.
   *
   * @return {ZmqProtocolSocket}
  **/
  close() {
    clearImmediate(this[deferFlush$]);
    this[deferFlush$] = null;
    const socket = this.socket;
    if (socket) {
      socket.removeAllListeners('error');
      socket.removeAllListeners('drain');
      socket.removeAllListeners('frames');
    }
    super.close();
    // reject all handlers and clear timeouts
    for (var handler of this[handlers$].values()) {
      handler.reject(new Error("closed"));
    }
    assert(this[handlers$].size === 0);
    var pendingSize = this[queues$].size;
    if (pendingSize !== 0) {
      debug('some messages still in pending queues: %s', pendingSize);
      this[queues$].clear();
    }
    return this;
  }

  /**
   * Connect socket.
   *
   * This method is implicitly called by ZmqProtocolSocket.prototype.request method
   * if an instance of ZmqProtocolSocket was initialized lazily.
   *
   * @return {ZmqProtocolSocket}
  **/
  connect() {
    if (this[connected$]) return;
    var socket = this.socket || (this.socket = new ZmqDealerSocket());
    /* makes sure socket is really closed when close() is called */
    socket.setsockopt(ZMQ_LINGER, 0);
    /* set some socket options */
    for(let [opt, val] of this[sockopts$]) socket.setsockopt(opt, val);
    this[connectUrls$](this.urls);
    this[connected$] = true;

    /* error handler */
    socket.on('error', err => {
      for (var handler of this[handlers$].values()) {
        handler.reject(err);
      }
    });
    /* flush handler */
    socket.on('drain', () => this[flush$]());
    /* response handler */
    socket.on('frames', (args) => {
      const requestId = args.shift()
         ,  requestKey = decodeRequestId(requestId);

      if (requestKey === undefined) {
        debug("socket.recv: invalid request id");
        return; /* ignore */
      }

      /* get the handler associated with request id */
      const handler = this[handlers$].get(requestKey);
      if (handler === undefined) {
        debug("socket.recv: unexpected request id: %s in response, will drop the response", requestKey);
        return; /* ignore */
      }
      const onresponse = handler.onresponse;

      /* optionally decode response */
      const protocol = handler.protocol;
      if (protocol !== undefined) args = protocol.decodeResponse(args);

      var result;

      if (onresponse !== undefined) {
        /* handle onresponse callback */
        try {
          result = onresponse(args, (msg) => {
            if (protocol !== undefined) msg = protocol.encodeRequest(msg);
            this[send$](requestKey, [requestId].concat(msg), true);
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
   * @property pendingRequests {number}
  **/
  get pendingRequests() {
    return this[handlers$].size;
  }

  /**
   * @property pendingQueues {number}
  **/
  get pendingQueues() {
    return this[queues$].size;
  }

  /**
   * Wait for all queues to drain.
   *
   * Call before close() or destroy() to make sure all outbound data has been flushed.
   * The returned promise resolves when data has been flushed or is rejected on timeout.
   *
   * @param {number} timeout - how long to wait in milliseconds.
   * @return {Promise}
  **/
  waitForQueues(timeout) {
    var queues = this[queues$];
    if (queues.size === 0) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const socket = this.socket;
      var ts = setTimeout(() => {
        socket.removeListener('flushed', flush);
        reject(new TimeoutError());
      }, timeout),
      flush = () => {
        clearTimeout(ts);
        resolve();
      };
      socket.once('flushed', flush);
    });
  }

  /* try sending immediately, when unsuccessfull queue */
  [send$](requestKey, payload, sendLater) {
    const queues = this[queues$]
        , isFlushing = this[isFlushing$];

    if (sendLater || isFlushing || queues.size !== 0) {
      let queue = queues.get(requestKey);
      if (queue === undefined) {
        queues.set(requestKey, [payload]);
      }
      else {
        queue.push(payload);
      }
      if (!isFlushing && !this[deferFlush$]) {
        this[deferFlush$] = setImmediate(() => {
          this[deferFlush$] = null;
          this[flush$]()
        });
      }
    }
    else {
      /* try to send now, omit queue, block [flush$] and force [send$] to queue if called */
      this[isFlushing$] = true;
      try {
        if (!this.socket.send(payload)) {
          /* no luck sending, [send$] will now queue payload */
          this[send$](requestKey, payload);
        }
      } finally {
        /* unblock */
        this[isFlushing$] = false;
      }
    }
  }

  /* flushes message queues, also called on 'drain' event */
  [flush$]() {
    if (this[isFlushing$]) return;
    this[isFlushing$] = true;

    const queues = this[queues$]
        , socket = this.socket;

    try {

      for(let [requestKey, queue] of queues) {
        let i;
        for(i = 0; i < queue.length; ++i) {
          /* queues.size and queue.length may grow while in socket.send */
          if (!socket.send(queue[i])) break;
        }
        if (i < queue.length) {
          /* unsuccessfull send, remove sent payloads */
          if (i === 1) queue.shift();
          else if (i !== 0) queue.splice(0, i);
          /* exit main loop */
          break;
        }
        else {
          /* queue is empty, remove */
          queues.delete(requestKey);
        }
      }

      /* no more messages, stop 'drain' events */
      if (queues.size === 0) {
        socket.cancelSend();
        socket.emit('flushed');
      }

    } catch(err) {
      for (var handler of this[handlers$].values()) {
        handler.reject(err); /* clears handlers and queues */
      }
    } finally {
      this[isFlushing$] = false;
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
    else if ('string' === typeof id && id.length === 24) {
      req.buf = Buffer.from(id, 'hex');
      req.key = id;
    }
    else {
      throw new TypeError("id must be a 12-byte buffer instance or a hex string");
    }
    return req;
  }

}

ZmqProtocolSocket.ZmqProtocolSocket = ZmqProtocolSocket;
ZmqProtocolSocket.TimeoutError = TimeoutError;
module.exports = exports = ZmqProtocolSocket;
