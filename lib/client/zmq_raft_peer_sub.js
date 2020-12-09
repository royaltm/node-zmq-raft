/* 
 *  Copyright (c) 2020 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

/*

Example:

  let client = new ZmqRaftPeerSub('tcp://127.0.0.1:8047', {broadcastTimeout: 250, lazy: true})
    .on('error', err => console.error(err))
    .on('pub', url => console.log("got PUB url: %s", url))
    .on('pulse', (lastTx, term, entries) => {
      console.log("got a pulse: last tx: %s term: %s new entries: %s", lastTx, term, entries.length)
    })
    .on('timeout', () => console.log("no heartbeat, an election is pending or the peer is down"))
    .on('close', () => console.log("client was closed"))

  client.close(); // shutdown all pending requests, streams and sockets

  // forever tx stream

  let subs = client.foreverEntriesStream(lastIndex, {timeout, snapshotOffset});

  subs.on('data', (entry) => {
    //...
  })
  .on('error', err => {
    if (err.isTimeout) {
      console.error("the peer is down");
    }
    else {
      console.error(err)
    }
  })
  .on('end', err => console.log("the peer is not a leader anymore"))
  .on('close', err => console.log("streaming closed"))

  // to shut this log stream down and free resources, when no longer needed
  subs.destroy(); 
*/
const isArray  = Array.isArray
    , isBuffer = Buffer.isBuffer

const assert = require('assert');
const { EventEmitter } = require('events');
const { Readable } = require('stream');

const { ZMQ_LINGER } = require('zeromq');
const { ZmqSocket } = require('../utils/zmqsocket');

const { assertConstantsDefined } = require('../utils/helpers');

const { bufferToLogEntry } = require('../common/log_entry');

const { BROADCAST_HEARTBEAT_INTERVAL
      } = require('../common/constants');

assertConstantsDefined({
  BROADCAST_HEARTBEAT_INTERVAL
}, 'number');

const DEFAULT_BROADCAST_TIMEOUT = BROADCAST_HEARTBEAT_INTERVAL * 2;
const BROADCAST_TIMEOUT_MIN = 100;

const REQUEST_URL_MSG_TYPE = '*';

const requestUrlTypeBuf  = Buffer.from(REQUEST_URL_MSG_TYPE);

const ZmqRaftPeerClient = require('../client/zmq_raft_peer_client');

const secretBuf$ = Symbol('secretBuf');

const { createFramesProtocol } = require('../protocol');

const stateBroadcastProtocol = createFramesProtocol('StateBroadcast');

const debug = require('debug')('zmq-raft:peer-sub');

/**
 * ZmqRaftPeerSub extends the single peer 0MQ Raft RPC protocol, providing a subscriber socket
 * that connects to the BroadcastStateMachine for the current RAFT log and state notifications.
 *
 * Inherits all of the ZmqRaftPeerClient methods and provides additional functions related to BSM.
 *
 * ZmqRaftPeerSub also emits the following events (as EventEmitter):
 *
 * - "error" (Error) - when an error occured while trying to subscribe to the BSM.
 * - "close" () - when the client instance is being closed; this event is being used to shut down
 *                all pending ForeverEntriesStream instances.
 * - "pub" (url: string) - emitted when the BSM publisher's `url` has been discovered.
 * - "pulse" (lastIndex: number, currentTerm: number, entries: Array) - emitted when a notification
 *           arrives from the BSM.
 * - "timeout" () - emitted when the BSM didn't sent a notification for the period of time exceeding
 *                  `broadcastTimeout`. This event is emitted only once, after the last received
 *                  notification or after subscribing to the BSM publisher socket.
 *                  It's possible to extend the timeout period by calling `refreshPulseTimeout()`.
**/
class ZmqRaftPeerSub extends ZmqRaftPeerClient {
  /**
   * Create ZmqRaftPeerSub
   *
   * `options` may be one of:
   *
   * - `url` {string}: A single cluster peer url to connect directly to.
   * - `urls` {Array<string>}: An array of url peers; each request will be send to each one of them
   *                           in a round-robin order.
   * - `secret` {string|Buffer}: A cluster identifying string which is sent and verified against
   *                             in each message.
   * - `timeout` {int32}: A default time interval in milliseconds after which we consider a server
   *           peer to be unresponsive; if waiting for the response exceeds this interval the request
   *           will timeout; 0 or less than 0 disables the request timeout - in this instance the
   *           response is waited for indefinitely;
   *           if undefined, the default is 500 ms.
   * - `broadcastTimeout` {int32}: The number of milliseconds before the BSM is considered unresponsive
   *                               (default: 1 second).
   * - `lazy` {boolean}: Specify `true` to connect lazily on the first request.
   * - `sockopts` {Object}: Specify zeromq socket options as an object e.g.: `{ZMQ_IPV4ONLY: true}`.
   * - `highwatermark` {number}: A shortcut to specify `ZMQ_SNDHWM` socket option for an underlying
   *                   zeromq DEALER socket; this affects how many messages are queued per server
   *                   so if one of the peers goes down this many messages are possibly lost;
   *                   setting it prevents spamming a peer with expired messages when temporary
   *                   network partition occures (default: 2).
   *
   * @param {string|Array<string>} [urls]
   * @param {Object} [options]
   * @return {ZmqRaftPeerSub}
  **/
  constructor(urls, options) {
    if (urls && !isArray(urls) && 'object' === typeof urls) {
      options = urls, urls = undefined;
    }
    options = Object.assign({lazy: false}, options);

    var broadcastTimeoutMs = (options.broadcastTimeout|0) || DEFAULT_BROADCAST_TIMEOUT;
    if (broadcastTimeoutMs < BROADCAST_TIMEOUT_MIN) {
      broadcastTimeoutMs = BROADCAST_TIMEOUT_MIN;
    }

    super(urls, options);
    EventEmitter.call(this);

    this.broadcastTimeoutMs = broadcastTimeoutMs;
    debug('broadcast timeout: %s ms.', broadcastTimeoutMs);

    this[secretBuf$] = Buffer.from(options.secret || '');
    this.url = null;
    this.lastLogIndex = 0;
    this.currentTerm = 0;
    this.isLeader = false;

    var sub = this.sub = new ZmqSocket('sub');
    /* makes sure socket is really closed when close() is called */
    sub.setsockopt(ZMQ_LINGER, 0);

    this._listener = stateBroadcastProtocol.createSubMessageListener(sub, handleBroadcast, this);
    this._pulseTimeout = null;
    this._subscribing = null;

    if (!options.lazy) {
      this.subscribe().catch(err => this.emit('error', err));
    }
  }

  /**
   * The last known publisher url. Updated after a successfull discovery request.
   * @property url {string|null}
  **/

  /**
   * The last known peer's log index. Updated before emitting each "pulse" event.
   * @property lastLogIndex {number}
  **/

  /**
   * The last known peer's current term. Updated before emitting each "pulse" event.
   * @property currentTerm {number}
  **/

  /**
   * The last known peer's state. Updated before emitting each "pulse" or "timeout" event.
   * @property isLeader {boolean}
  **/

  toString() {
    var url = this.url;
    return `[object ZmqRaftPeerSub{${(url || '-none-')}}]`;
  }

  /**
   * Disconnect, close socket and reject all pending requests.
   *
   * This also closes all instances of ForeverEntriesStream.
   * 
   * @return {ZmqRaftPeerSub}
  **/
  close() {
    const sub = this.sub;
    if (!sub) return this;
    this.sub = null;
    clearTimeout(this._pulseTimeout);
    this._pulseTimeout = null;
    sub.unsubscribe(this[secretBuf$]);
    if (this.url != null) {
      debug('sub.disconnect: %s', this.url)
      try {
        sub.disconnect(this.url);
      }
      catch(_e) {/* ignore */}
    }
    sub.removeListener('frames', this._listener);
    sub.close();
    try {
      this.emit('close');
    }
    catch(err) {
      console.error(err);
    }
    debug('subscriber closed');
    return super.close();
  }

  /**
   * Discover BroadcastStateMachine publisher's URL.
   * 
   * The returned promise resolves to a string with the publisher's url.
   * 
   * @param {int32} [timeout] - an override for the response timeout in milliseconds;
   *                the promise is rejected with TimeoutError on timeout;
   *                if this is 0, null or undefined, default timeout is used instead.
   * @return {Promise}
  **/
  requestBroadcastStateUrl(timeout) {
    return this.request([requestUrlTypeBuf, this[secretBuf$]], { timeout })
    .then(resp => {
      if (isArray(resp) && resp.length === 1) {
        const urlbuf = resp[0];
        if (isBuffer(urlbuf)) return urlbuf.toString();
      }
      throw new Error("broadcast state url missing in the response");
    });
  }

  /**
   * Unsubscribe from the BroadcastStateMachine publisher's socket.
   *
   * Calling this method stops "pulse" events and will also prevent "timeout" events.
   *
   * After calling this method, a call to `subscribe()` is explicitly needed to reconnect
   * and re-subscribe the socket.
  **/
  unsubscribe() {
    const sub = this.sub
        , url = this.url;
    if (url && sub) {
      debug('sub.disconnect: %s', this.url)
      try {
        sub.disconnect(this.url);
      }
      catch(_e) {/* ignore */}
      sub.unsubscribe(this[secretBuf$]);
      clearTimeout(this._pulseTimeout);
      this._pulseTimeout = null;
    }
  }

  /**
   * Subscribe to the BroadcastStateMachine publisher's socket.
   *
   * May attempt to discover the publisher's `url`. In this instance the `timeout` parameter
   * is used for the discovery RPC.
   *
   * The returned promise resolves to the BSM publisher's URL.
   *
   * @return {Promise}
  **/
  subscribe(timeout) {
    if (this._subscribing) return this._subscribing;

    const sub = this.sub
        , url = this.url;

    if (!sub) return Promise.reject(new Error("ZmqRaftPeerSub.subscribe: already closed"));

    if (url) {
      if ('string' !== typeof url) {
        return Promise.reject(new TypeError("ZmqRaftPeerSub.subscribe: url must be a string or a buffer"));
      }
      debug('subscriber.connect: %s', url);
      sub.connect(url);
      sub.subscribe(this[secretBuf$]);
      clearTimeout(this._pulseTimeout);
      this._pulseTimeout = true;
      this.refreshPulseTimeout();
      return Promise.resolve(url);
    }

    return this._subscribing = this.requestBroadcastStateUrl(timeout)
    .then(url => {
      this.url = url;
      this._subscribing = null;
      try {
        this.emit('pub', url);
      } catch(err) {
        console.error(err)
      }
      return this.subscribe(timeout);
    },
    err => {
      this._subscribing = null;
      throw err;
    });
  }

  /**
   * Restart BroadcastStateMachine timeout counter.
  **/
  refreshPulseTimeout() {
    if (this._pulseTimeout) {
      clearTimeout(this._pulseTimeout);
      if (this.sub === null) return;
      this._pulseTimeout = setTimeout(() => {
        debug('broadcast timeout');
        this.isLeader = false;
        try {
          this.emit('timeout');
        }
        catch(err) {
          this.emit('error', err);
        }
      }, this.broadcastTimeoutMs);
    }
  }

  /**
   * Returns a Readable stream that retrieves RAFT log entries via the combination of
   * RequestEntries RPC and notifications from BroadcastStateMachine.
   *
   * The stream ends when the RAFT peer (that this client connects to) is not a LEADER.
   * Otherwise, the stream, after pushing all the present requested log entries, waits
   * for the future updates via BSM notifications, and pushes all incoming new entries.
   * 
   * The returned stream is lazy: RequestEntries RPC messages are sent to the peer server only
   * when data is requested via stream.Readable api.
   * 
   * The returned stream can be back-pressured.
   *
   * The stream yields objects that are instances of either common.LogEntry or common.SnapshotChunk.
   * 
   * NOTICE: If the stream is no longer needed, call its `destroy()` method (from Readable api).
   *         Otherwise, it will leak resources. There is no need to call this method if the stream
   *         ends (because of the peer's LEADER state change) or emits an error.
   * 
   * `options`:
   * 
   * - `timeout` {int32} - an override for the response timeout in milliseconds;
   *                       the stream will emit an "error" event with TimeoutError on timeout;
   *                       if this is 0, null or undefined, default timeout is used instead.
   * - `snapshotOffset` {number} - a hint for the server to start responding with snapshot chunks starting
   *                  from this byte offset, providing the `lastIndex` precedes the actual snapshot index.
   * 
   * Any other option is passed to the Readable constructor.
   * 
   * @param {number} lastIndex - index of the log entry PRECEDING entries that are actually requested
   *                             (e.g. the index of the last received entry), specify 0 to start from 1.
   * @param {Object} [options]
   * @return {ForeverEntriesStream}
  **/
  foreverEntriesStream(lastIndex, options) {
    if (!this.url) {
      this.subscribe().catch(err => this.emit('error', err));
    }
    return new ForeverEntriesStream(this, lastIndex, options);
  }
}

function handleBroadcast(args) {
  const [secret, term, lastIndex] = args.splice(0, 3)
      , entries = args;

  if (!this[secretBuf$].equals(secret)) {
    return this.emit('error', new Error('ZmqRaftPeerSub: broadcast authentication fails'));
  }

  this.lastLogIndex = lastIndex;
  this.currentTerm = term;
  this.isLeader = true;

  this.refreshPulseTimeout();

  try {
    this.emit('pulse', lastIndex, term, entries);
  }
  catch(err) {
    this.emit('error', err);
  }
}

/* ZmqRaftPeerSub mixin EventEmitter */
for (let prop in EventEmitter.prototype) {
  ZmqRaftPeerSub.prototype[prop] = EventEmitter.prototype[prop];
}

class ForeverEntriesStream extends Readable {
  constructor(client, lastIndex, options) {
    var {timeout, snapshotOffset} = options || {}; // TODO: count perhaps?

    super(Object.assign({}, options, {objectMode: true}));

    this.client = client;
    this.isFlowing = false;
    this._ahead = [];
    this._rlStream = null;

    if (lastIndex === undefined) lastIndex = 0;
    else if (!Number.isFinite(lastIndex) || lastIndex < 0 || lastIndex % 1 !== 0) {
      throw new Error("ForeverEntriesStream: lastIndex must be an unsigned integer");
    }

    this.sentIndex = lastIndex;

    client.on('error', this._errorHandler = (err) => this.destroy(err));
    client.on('close', this._closeHandler = () => this.destroy());
    client.on('timeout', this._timeoutHandler = () => {
      // trigger missing entries so we can determine if the peer is really down or is not a leader
      this._recvMissingEntries();
    });

    this._pulseHandler = (lastIndex, _term, entries) => {
      var pushedEntries = false;
      if (this._rlStream == null) {
        pushedEntries = this._pushEntries(lastIndex, entries);
      }
      if (pushedEntries === false && lastIndex > this.sentIndex) {
        if (entries.length !== 0) this._ahead.push({lastIndex, entries});
        this._recvMissingEntries();
      }
    };

    this._recvMissingEntries = () => {
      if (this._rlStream != null) return;
      debug("recv missing entries since: %s", this.sentIndex);
      this._rlStream = client.requestEntriesStream(this.sentIndex, {timeout, snapshotOffset})
      .on('data', chunk => {
        var logIndex = chunk.logIndex;
        if (logIndex > this.sentIndex) {
          if (chunk.isLogEntry || chunk.isLastChunk) {
            this.sentIndex = logIndex;
            snapshotOffset = 0;
          }
          else if (chunk.isSnapshotChunk) {
            snapshotOffset = chunk.snapshotByteOffset + chunk.length;
          }
          if (!this.push(chunk)) {
            this._stopFlow();
          }
        }
      })
      .on('end', () => {
        this._rlStream = null;
        // flush ahead
        while (this._ahead.length !== 0) {
          let {lastIndex, entries} = this._ahead.shift();
          this._pushEntries(lastIndex, entries);
        }
      })
      .on('error', err => { //TODO: think if should handle out of order?
        if (err.isPeerNotLeader) {
          debug("recessed peer - no more a LEADER, forever ends");
          this.push(null); // peer is not a LEADER anymore, just end the stream
        }
        else {
          this.destroy(err);
        }
      })
    }

  }

  _destroy(err, callback) {
    const client = this.client;
    this.isFlowing = null;
    client.removeListener('error', this._errorHandler);
    client.removeListener('close', this._closeHandler);
    client.removeListener('timeout', this._timeoutHandler);
    client.removeListener('pulse', this._pulseHandler);
    if (this._rlStream) {
      this._rlStream.destroy();
    }
    callback(err);
  }

  _read(_size) {
    const client = this.client;
    if (this.isFlowing === false) {
      this.isFlowing = true;
      client.on('pulse', this._pulseHandler);
    }
    if (this._rlStream) {
      this._rlStream.resume();
    }
    else if (!client.isLeader || this.sentIndex < client.lastLogIndex) {
      this._recvMissingEntries(); // if not really a LEADER this will trigger the stream to end
    }
  }

  _stopFlow() {
    if (this.isFlowing) {
      this.isFlowing = false;
      this.client.removeListener('pulse', this._pulseHandler);
      if (this._rlStream) {
        this._rlStream.pause();
      }
      this._ahead.length = 0;
    }
  }

  // returns num entries pushed, or false on index mismatch
  _pushEntries(lastIndex, entries) {
    const numEntries = entries.length;
    var more = true;
    var index = lastIndex - numEntries;

    if (index === this.sentIndex) {
      for(let i = 0; i < numEntries; ++i) {
        more = this.push( bufferToLogEntry(entries[i], ++index) );
      }
      this.sentIndex = lastIndex;
      if (!more) {
        this._stopFlow();
      }
      return numEntries;
    }
    else {
      return false;
    }
  }
}

ZmqRaftPeerSub.ZmqRaftPeerSub = ZmqRaftPeerSub;
ZmqRaftPeerSub.ForeverEntriesStream = ForeverEntriesStream;
module.exports = exports = ZmqRaftPeerSub;
