/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

/*

  client = new ZmqRaftSubscriber('tcp://127.0.0.1:1234')

  client.on('data' => ([id, ...args]) => {
  
  })

  client.write([id, ...args])

  db.stream.pipe(client).pipe(db.stream);

*/

const isArray  = Array.isArray
    , isBuffer = Buffer.isBuffer
    , now      = Date.now;

const assert = require('assert');

const Duplex = require('stream').Duplex;

const { ZMQ_LINGER } = require('zmq');
const { ZmqSocket } = require('../utils/zmqsocket');

const { assertConstantsDefined, delay, parsePeers } = require('../utils/helpers');

const { LogEntry, bufferToLogEntry, UpdateRequest: { isUpdateRequest } } = require('../common/log_entry');
const { SnapshotChunk, bufferToSnapshotChunk } = require('../common/snapshot_chunk');

const { SERVER_RESPONSE_TTL
      , RE_STATUS_SNAPSHOT
      , SERVER_ELECTION_GRACE_MS
      } = require('../common/constants');

assertConstantsDefined({
  SERVER_RESPONSE_TTL
, RE_STATUS_SNAPSHOT
, SERVER_ELECTION_GRACE_MS
}, 'number');

const REQUEST_URL_MSG_TYPE = '*';

const requestUrlTypeBuf  = Buffer.from(REQUEST_URL_MSG_TYPE);

const ZmqRaftClient = require('../client/zmq_raft_client');

const TimeoutError = ZmqRaftClient.TimeoutError;

const secretBuf$ = Symbol('secretBuf');

const { createFramesProtocol } = require('../protocol');

const stateBroadcastProtocol = createFramesProtocol('StateBroadcast');

const debug = require('debug')('zmq-raft:subscriber');

function MissingEntriesError(message) {
  Error.captureStackTrace(this, TimeoutError);
  this.name = 'TimeoutError';
  this.message = message || 'some entries are missing';
}

MissingEntriesError.prototype = Object.create(Error.prototype);
MissingEntriesError.prototype.constructor = MissingEntriesError;
MissingEntriesError.prototype.isMissingEntries = true;

class ZmqRaftSubscriber extends Duplex {

  /**
   * Create ZmqRaftSubscriber
   *
   * `options` may be one of:
   *
   * - `url` {String}: an url to fetch peers from
   * - `urls` {Array}: an of urls to fetch peers from
   * - `peers` {Array}: array of raft server descriptors
   * - `secret` {String}: a secret to send to servers and subscribe to
   * - `lastIndex` {number}: index of last entry in local state machine
   * - `sockopts` {Object}: specify zmq socket options as object e.g.: {ZMQ_IPV4ONLY: true}
   * - `highwatermark` {number}: shortcut to specify ZMQ_SNDHWM for a zmq DEALER socket
   *   this affects how many messages are queued per server so if one of the peers
   *   goes down this many messages are possibly lost
   *   (unless the server goes on-line and responds within the request timeout)
   *   this prevents to spam server with expired messages when temporary network partition occures
   *   default is 1
   *
   * @param {string|Array} [urls]
   * @param {Object} [options]
   * @return {ZmqRaftSubscriber}
  **/
  constructor(urls, options) {
    if (urls && !isArray(urls) && 'object' === typeof urls) {
      options = urls, urls = undefined;
    }
    options || (options = {});
    if (!options && 'object' !== typeof options) {
      throw TypeError('ZmqRaftSubscriber: options must be an object');
    }

    const client = new ZmqRaftClient(urls, Object.assign({lazy: true}, options));

    super({objectMode: true, highWaterMark: 16});

    this._read = (/* size */) => {
      /* lazy initialize */
      delete this._read;
      delete this._write;
      sub.subscribe(this[secretBuf$]);
      this._subscriberTimeout = true;
      this._requestPublisherUrl().catch(err => this.emit('error', err));
    };

    this._write = (updateRequest, encoding, callback) => {
      delete this._write;
      this._requestPublisherUrl().catch(err => this.emit('error', err));
      this._write(updateRequest, encoding, callback);
    };

    this.client = client;

    this[secretBuf$] = Buffer.from(String(options.secret || ''));

    this.url = null;

    var sub = this.sub = new ZmqSocket('sub');
    /* makes sure socket is really closed when close() is called */
    sub.setsockopt(ZMQ_LINGER, 0);

    this._listener = stateBroadcastProtocol.createSubMessageListener(sub, this._handleBroadcast, this);

    var lastIndex = options.lastIndex;

    if (lastIndex === undefined) lastIndex = 0;
    else if (!Number.isFinite(lastIndex) || lastIndex < 0 || lastIndex % 1 !== 0) {
      throw new Error("ZmqRaftSubscriber: lastIndex must be an unsigned integer");
    }

    this.lastLogIndex = lastIndex;
    this.ahead = [];
    this.isFresh = null;
    this._pendingMissing = null;
    this._pendingRequestPubUrl = null;
    this._subscriberTimeout = undefined;
  }

  close() {
    const sub = this.sub;
    if (!sub) return this;
    this.sub = null;
    clearTimeout(this._subscriberTimeout);
    this._subscriberTimeout = undefined;
    sub.unsubscribe(this[secretBuf$]);
    if (this.url !== null) {
      debug('socket.disconnect: %s', this.url)
      sub.disconnect(this.url);
    }
    sub.removeListener('frames', this._listener);
    sub.close();
    this.client.close();
    this.client = null;
    return this;
  }

  toString() {
    var url = this.url;
    return `[object ZmqRaftSubscriber{${(url || '-none-')}}]`;
  }

  connect(url) {
    const sub = this.sub
        , old = this.url;

    if (isBuffer(url)) url = url.toString();

    if (url !== old && old) {
      debug('subscriber.disconnect: %s', old);
      sub.disconnect(old);
    }

    if (url) {
      if ('string' !== typeof url) {
        throw new TypeError("subscriber.connect: url must be a string or buffer");
      }
      this.url = url;
      debug('subscriber.connect: %s', url);
      sub.connect(url);
    }
    else {
      this.url = null;
    }
    return this;
  }

  _requestPublisherUrl() {
    if (this._pendingRequestPubUrl !== null) return this._pendingRequestPubUrl;

    const client = this.client;

    var request = () => {
      return client.requestConfig().then(() => {
        if (client.leaderId !== null) {
          /* we have a leader, ask for state machine publisher url */
          return client.request([requestUrlTypeBuf, this[secretBuf$]])
          .then(([url]) => {
            this.connect(url);
            if (url === undefined) {
              return request();
            }
            else {
              this._refreshSubTimeout();
              /* success */
              this._pendingRequestPubUrl = null;
              return this.url;
            }
          })
          .catch(err => {
            if (err.isTimeout) {
              debug('timeout occured trying to find another server');
              client.setLeader(null);
              return request();
            }
            else throw err;
          });
        }
        else {
          debug('no leader, trying again later');
          return delay(SERVER_ELECTION_GRACE_MS).then(() => request());
        }
      });
    };

    return this._pendingRequestPubUrl = request().catch(err => {
      this._pendingRequestPubUrl = null;
      throw err;
    });
  }

  _handleBroadcast(args) {
    const [secret, term, lastLogIndex] = args.splice(0, 3);
    const entries = args;
    if (!this[secretBuf$].equals(secret)) {
      return this.emit('error', new Error('ZmqRaftSubscriber: broadcast auth fail'));
    }
    try {
      if (!this._appendEntries(entries, lastLogIndex)) {
        this._pauseSubscriber();
      }
      if (!this.isFresh) {
        this.isFresh = true;
        this.emit('fresh');
      }
    } catch(err) {
      if (err.isMissingEntries) {
        debug('broadcast missing: (%s) %s < %s', entries.length, this.lastLogIndex, lastLogIndex);
        const ahead = this.ahead;
        if (entries.length !== 0) {
          ahead.push({lastLogIndex: lastLogIndex, entries: entries});
        }
        this._requestMissingEntries(lastLogIndex - entries.length - this.lastLogIndex);
      }
      else throw err;
    }
    this._refreshSubTimeout();
  }

  _flushAhead() {
    const ahead = this.ahead;
    while(ahead.length !== 0) {
      const {lastLogIndex, entries} = ahead[0];
      try {
        if (!this._appendEntries(entries, lastLogIndex)) {
          this._pauseSubscriber();
        }
        ahead.shift();
      } catch(err) {
        if (err.isMissingEntries) {
          /* gap in ahead */
          return this._requestMissingEntries(lastLogIndex - entries.length - this.lastLogIndex);
        }
        else throw err;
      }
    }
  }

  /*
    throws MissingEntriesError if provided entries can't be applied
    returns false when backpressure is needed:
    https://nodejs.org/dist/latest-v6.x/docs/api/stream.html#stream_readable_push_chunk_encoding */
  _appendEntries(entries, lastLogIndex) {
    const entryCount = entries.length;
    const gapSize = lastLogIndex - this.lastLogIndex;
    if (gapSize > entryCount) {
      throw new MissingEntriesError();
    }
    var res = true;
    if (lastLogIndex > this.lastLogIndex) {
      const firstLogIndex = lastLogIndex - entryCount + 1;
      debug('appending entries: (%s) indexes: %s - %s', entryCount, firstLogIndex, lastLogIndex);
      for(let i = entryCount - gapSize; i < entryCount; ++i) {
        let entry = bufferToLogEntry(entries[i], firstLogIndex + i);
        res = this.push(entry);
      }
      this.lastLogIndex = lastLogIndex;
    }
    return res;
  }

  _pauseSubscriber() {
    if (this._subscriberTimeout) {
      clearTimeout(this._subscriberTimeout);
      debug('backpressured, unsubscribing');
      const sub = this.sub;
      sub.pause();
      sub.unsubscribe(this[secretBuf$]);
      this._subscriberTimeout = null;
    }
  }

  _refreshSubTimeout() {
    if (this._subscriberTimeout) {
      clearTimeout(this._subscriberTimeout);
      if (this.sub === null) return;
      this._subscriberTimeout = setTimeout(() => {
        this.emit('timeout');
        this._requestPublisherUrl().catch(err => this.emit('error', err));
      }, SERVER_RESPONSE_TTL*2);
    }
  }

  _requestMissingEntries(count) {
    assert(count > 0);

    if (this._pendingMissing !== null) return this._pendingMissing;

    if (this._subscriberTimeout === null) return Promise.resolve();

    this.isFresh = false;
    this.emit('stale', count);

    return this._pendingMissing = this.client.requestEntries(this.lastLogIndex, count
      , (status, entriesOrChunk, logIndex, byteOffset, snapshotSize, isLastChunk) => {
        var more;
        if (status === RE_STATUS_SNAPSHOT) {
          if (logIndex > this.lastLogIndex) {
            /* warning: back pressuring will require to re-send whole snaphot */
            more = this.push(bufferToSnapshotChunk(entriesOrChunk, logIndex, byteOffset, snapshotSize));
            if (isLastChunk) this.lastLogIndex = logIndex;
          }
          else more = true;
        }
        else {
          more = this._appendEntries(entriesOrChunk, logIndex);
        }

        if (!more) {
          this._pauseSubscriber();
          return false; /* cancel requestEntries */
        }

    })
    .then(() => {
      this._pendingMissing = null;
      return this._flushAhead();
    })
    .catch(err => {
      this._pendingMissing = null;
      if (err.isOutOfOrder) {
        debug(err.message);
        return this._flushAhead();
      }
      this.emit('error', err)
    });
  }

  /* Readable */

  _read(/* size */) {
    if (this._subscriberTimeout === null) {
      debug('resuming streaming - ahead: %s lastLogIndex: %s', this.ahead.length, this.lastLogIndex);
      const sub = this.sub;
      sub.resume();
      sub.subscribe(this[secretBuf$]);
      this._subscriberTimeout = true;
      this._refreshSubTimeout();
    }
  }

  /* Writable */

  _write(updateRequest, encoding, callback) {
    assert(isUpdateRequest(updateRequest), "updateRequest must be a buffer with requestId property");
    this.client.requestUpdate(updateRequest.requestId, updateRequest).then(res => {
      debug('written index: %s', res);
      callback();
    }, callback);
  }

}

ZmqRaftSubscriber.ZmqRaftSubscriber = ZmqRaftSubscriber;
module.exports = exports = ZmqRaftSubscriber;
