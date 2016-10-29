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

const { createUnzip } = require('zlib');

const { decode, createDecodeStream, Encoder } = require('msgpack-lite');

const { ZMQ_LINGER } = require('zmq');
const { ZmqSocket } = require('../utils/zmqsocket');

const { assertConstantsDefined, delay, parsePeers } = require('../utils/helpers');

const { LogEntry } = require('../common/log_entry');

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

const debug = require('debug')('raft-subscriber');

const anonymousRequestEntryTemplate = [Symbol("anonymous")];

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
   * - `unzip` {bool}: unzip snapshot (default: true)
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
    if (urls && 'object' === typeof urls) {
      options = urls, urls = undefined;
    }
    options || (options = {});
    if (!options && 'object' !== typeof options) {
      throw TypeError('ZmqRaftSubscriber: options must be an object');
    }

    const client = new ZmqRaftClient(urls, Object.assign({lazy: true}, options));

    super({objectMode: true});

    this._read = (/* size */) => {
      /* lazy initialize */
      delete this._read;
      this._requestPublisherUrl().catch(err => this.emit('error', err));
    };

    this.client = client;

    this[secretBuf$] = Buffer.from(String(options.secret || ''));

    this.unzip = options.unzip !== false;

    this.url = null;

    var sub = this.sub = new ZmqSocket('sub');
    /* makes sure socket is really closed when close() is called */
    sub.setsockopt(ZMQ_LINGER, 0);

    sub.subscribe(this[secretBuf$]);

    this._listener = stateBroadcastProtocol.createSubMessageListener(sub, this._handleBroadcast, this);

    var lastIndex = options.lastIndex;

    if (lastIndex === undefined) lastIndex = 0;
    else if (!Number.isFinite(lastIndex) || lastIndex < 0 || lastIndex % 1 !== 0) {
      throw new Error("ZmqRaftSubscriber: lastIndex must be an unsigned integer");
    }

    this.lastLogIndex = lastIndex;
    this.ahead = [];
    this._encoder = new Encoder();
    this._pendingMissing = null;
    this._pendingRequestPubUrl = null;
    this._subscriberTimeout = null;
  }

  close() {
    const sub = this.sub;
    if (!sub) return Promise.resolve();
    this.sub = null;
    clearTimeout(this._subscriberTimeout);
    this._subscriberTimeout = null;
    sub.unsubscribe('');
    if (this.url !== null) {
      debug('socket.disconnect: %s', this.url)
      sub.disconnect(this.url);
    }
    sub.removeListener('message', this._listener);
    sub.close();
    this.client.close();
    this.client = null;
  }

  toString() {
    var url = this.url;
    return `[object ZmqRaftSubscriber{{(url || '-none-')}}]`;
  }

  connect(url) {
    const sub = this.sub
        , old = this.url;

    if (url !== old && old) {
      debug('subscriber.disconnect: %s', old);
      sub.disconnect(old);
    }

    if (url) {
      if (isBuffer(url)) url = url.toString();
      else if ('string' !== typeof url) {
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
    if (!this._appendEntries(entries, lastLogIndex)) {
      debug('broadcast missing: (%s) %s < %s', entries.length, this.lastLogIndex, lastLogIndex);
      const ahead = this.ahead;
      if (entries.length !== 0) {
        ahead.push({lastLogIndex: lastLogIndex, entries: entries});
      }
      this._requestMissingEntries(lastLogIndex - entries.length - this.lastLogIndex);
    }
    this._refreshSubTimeout();
  }

  _flushAhead() {
    const ahead = this.ahead;
    while(ahead.length !== 0) {
      const {lastLogIndex, entries} = ahead.shift();
      if (!this._appendEntries(entries, lastLogIndex)) {
        /* gap in ahead */
        return this._requestMissingEntries(lastLogIndex - entries.length - this.lastLogIndex);
      }
    }
    this.ahead.length = 0;
    this.emit('fresh');
  }

  _appendEntries(entries, lastLogIndex) {
    const entryCount = entries.length;
    const gapSize = lastLogIndex - this.lastLogIndex;
    if (gapSize <= entryCount) {
      if (lastLogIndex > this.lastLogIndex) {
        debug('appending entries: (%s) lastIndex: %s', entryCount, lastLogIndex);
        for(let i = entryCount - gapSize; i < entryCount; ++i) {
          let entry = new LogEntry(entries[i]);
          if (entry.isState) {
            let state = decode(entry.readData());
            this.push( [entry.readRequestKey()].concat(state) );
          }
        }
        this.lastLogIndex = lastLogIndex;
      }
      return true;
    }
    /* there are missing entries */
    return false;
  }

  _refreshSubTimeout() {
    clearTimeout(this._subscriberTimeout);
    if (this.sub === null) return;
    this._subscriberTimeout = setTimeout(() => {
      this.emit('timeout');
      this._subscriberTimeout = null;
      this._requestPublisherUrl().catch(err => this.emit('error', err));
    }, SERVER_RESPONSE_TTL*2);
  }

  _requestMissingEntries(count) {
    assert(count > 0);

    if (this._pendingMissing !== null) return this._pendingMissing;

    this.emit('stale', count);

    var decode, unzip, lastLogIndex, error;

    return this._pendingMissing = this.client.requestEntries(this.lastLogIndex, count,
      (status, entries, logIndex, byteOffset, snapshotSize) => {

        if (error) throw error;

        if (status === RE_STATUS_SNAPSHOT && (logIndex > this.lastLogIndex)) {
          if (byteOffset === 0) {
            lastLogIndex = logIndex;
            decode = createDecodeStream().on('error', err => (error = err));
            decode.on('data', state => this.push(anonymousRequestEntryTemplate.concat(state)));
            if (this.unzip) {
              unzip = createUnzip().on('error', err => (error = err));
              unzip.pipe(decode);
            }
            else unzip = decode;
          }
          unzip.write(entries);
        }
        else assert(this._appendEntries(entries, logIndex), 'missing entries out of order');
      }
    )
    .then(() => {
      if (error) return Promise.reject(error);
      if (unzip) {
        return new Promise((resolve, reject) => {
          decode.on('end', resolve);
          unzip.end(() => {
            this.lastLogIndex = lastLogIndex;
          });
        });
      }
    })
    .then(() => {
      this._pendingMissing = null;
      return this._flushAhead();
    })
    .catch(err => {
      this._pendingMissing = null;
      this.emit('error', err)
    });
  }

  /* Readable */

  _read(/* size */) {
    console.warn('_read');
  }

  /* Writable */

  _write(chunk, encoding, callback) {
    assert(isArray(chunk));
    const ident = chunk.shift()
        , encoder = this._encoder;
    encoder.write(chunk);
    this.client.requestUpdate(ident, encoder.read()).then(res => {
      debug('written index: %s', res);
      callback();
    }, callback);
  }

}

ZmqRaftSubscriber.ZmqRaftSubscriber = ZmqRaftSubscriber;
module.exports = exports = ZmqRaftSubscriber;
