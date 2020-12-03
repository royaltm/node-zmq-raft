/* 
 *  Copyright (c) 2016-2020 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray
    , now     = Date.now;

const assert = require('assert');

// const { Readable } = require('stream');

const { encode: encodeMsgPack } = require('msgpack-lite');

const { assertConstantsDefined, parsePeers } = require('../utils/helpers');
// const { bufferToLogEntry } = require('../common/log_entry');
// const { bufferToSnapshotChunk } = require('../common/snapshot_chunk');

/* expect response within this timeout, if not will try the next server */
const { SERVER_ELECTION_GRACE_DELAY

      // , RE_STATUS_NOT_LEADER
      // , RE_STATUS_LAST
      // , RE_STATUS_MORE
      // , RE_STATUS_SNAPSHOT

      // , REQUEST_ENTRIES
      } = require('../common/constants');

assertConstantsDefined({
  SERVER_ELECTION_GRACE_DELAY
// , RE_STATUS_NOT_LEADER
// , RE_STATUS_LAST
// , RE_STATUS_MORE
// , RE_STATUS_SNAPSHOT
}, 'number');

assertConstantsDefined({
 // REQUEST_ENTRIES
}, 'string', true);

// const requestEntriesTypeBuf = Buffer.from(REQUEST_ENTRIES);

// const { createFramesProtocol } = require('../protocol');

// const requestEntriesProtocol = createFramesProtocol('RequestEntries');

const ZmqRaftPeerClient = require('../client/zmq_raft_peer_client');
const TimeoutError = ZmqRaftPeerClient.TimeoutError;
const RequestEntriesStream = ZmqRaftPeerClient.RequestEntriesStream;

const rcPending$ = Symbol('rcPending');
const timeout$   = Symbol('timeout');

const debug = require('debug')('zmq-raft:client');

// function OutOfOrderError(message) {
//   Error.captureStackTrace(this, OutOfOrderError);
//   this.name = 'OutOfOrderError';
//   this.message = message || 'chunks received out of order';
// }

// OutOfOrderError.prototype = Object.create(Error.prototype);
// OutOfOrderError.prototype.constructor = OutOfOrderError;
// OutOfOrderError.prototype.isOutOfOrder = true;

/**
 * This client executes RPC calls on the 0MQ Raft cluster.
 * 
 * Overrides the ZmqRaftPeerClient RPC methods making them resilient to singular peer failures.
 * 
 * The `timeout` argument of all the RPC methods also changes meaning.
 * When the peer ZmqRaftPeerClient methods would just throw a timeout error waiting for the
 * single response, the RPC methods of ZmqRaftClient would repeat requests, to every known peer
 * in the cluster.
 * 
 * The `timeout` argument given to these RPC methods is an expiration timeout put on top
 * of the whole procedure. The timeout error will be thrown after possible several attemps
 * to contact several different cluster peers.
 * 
 * Also these RPC methods never throw PeerNotLeaderError. Instead they retry sending requests
 * to the new leader. When the election is still pending a client gives a little time (grace delay)
 * for the cluster to elect the new leader.
**/
class ZmqRaftClient extends ZmqRaftPeerClient {
  /**
   * Create an instance of ZmqRaftClient.
   *
   * `options` may be one of:
   *
   * - `url` {string}: A seed url to fetch peer urls from via a Request Config RPC.
   * - `urls` {Array<string>}: An array of seed urls to fetch peers from via a Request Config RPC.
   * - `peers` {Array}: An array of established zmq raft server descriptors;
   *                    `peers` has precedence over `urls` and if provided the peer list
   *                    is not being fetched via Request Config RPC.
   * - `secret` {string|Buffer}: A cluster identifying string which is sent and verified against
   *                             in each message.
   * - `timeout` {number}: A time interval in milliseconds after which we consider a server peer as unresponsive.
   *                       The request will be repeated if waiting for the response exceeds this interval.
   *                       In this case the next request will be send to the new leader or the next
   *                       peer in the cluster if the leader is not known.
   * - `serverElectionGraceDelay` {number}: A delay in milliseconds to wait for the cluster
   *                                        leader to elect before re-trying (default: 300).
   * - `lazy` {boolean}: Specify `true` to connect lazily on the first request.
   * - `heartbeat` {number}: How often, in milliseconds, to update cluster peer configuration
   *               via Request Config RPC; pass 0 to disable (default: 0); when establishing
   *               a long lasting connection consider this option mandatory.
   * - `sockopts` {Object}: Specify zeromq socket options as an object e.g.: `{ZMQ_IPV4ONLY: true}`.
   * - `highwatermark` {number}: A shortcut to specify `ZMQ_SNDHWM` socket option for an underlying
   *                   zeromq DEALER socket; this affects how many messages are queued per server
   *                   so if one of the peers goes down this many messages are possibly lost;
   *                   setting it prevents spamming a peer with expired messages when temporary
   *                   network partition occures (default: 2).
   *
   * @param {string|Array<string>} [urls]
   * @param {Object} [options]
   * @return {ZmqRaftClient}
  **/
  constructor(urls, options) {
    if (urls && !isArray(urls) && 'object' === typeof urls) {
      options = urls, urls = undefined;
    }
    options || (options = {});
    if (!options && 'object' !== typeof options)
      throw TypeError('ZmqRaftClient: options must be an object');

    urls || (urls = options.urls || options.url);
    var peers = options.peers ? parsePeers(options.peers) : new Map();
    if (peers.size === 0) {
      if (!urls || urls.length === 0) {
        throw Error('ZmqRaftClient: at least one peer or url should be provided');
      }
    }
    else {
      urls = Array.from(peers.values());
    }

    super(urls, options);

    this.peers = peers;
    this.leaderId = null;
    this[timeout$] = new Set();
    this.serverElectionGraceDelay = (options.serverElectionGraceDelay|0) || SERVER_ELECTION_GRACE_DELAY;
    this.heartbeatMs = options.heartbeat|0;
    if (this.heartbeatMs > 0) {
      this.heartbeat = null;
      const heartbeat = (ms) => {
        if (this.heartbeat !== undefined) {
          this.heartbeat = setTimeout(() => {
            this.requestConfig(this.timeoutMs*5)
            .catch(err => debug('heartbeat error: %s', err))
            .then(() => heartbeat(this.heartbeatMs));
          }, ms);
        }
      };
      debug('starting heartbeats every: %s ms', this.heartbeatMs);
      heartbeat(this[Symbol.for('connected')] ? 0 : this.heartbeatMs);
    }
  }

  /**
   * Disconnect, close socket and reject all pending requests.
   *
   * @return {ZmqRaftClient}
  **/
  close() {
    clearTimeout(this.heartbeat);
    this.heartbeat = undefined;
    for(let cancel of this[timeout$]) cancel(new Error("closed"));
    assert(this[timeout$].size === 0);
    return super.close();
  }

  /**
   * Create a cancellable time delay promise.
   *
   * The promise will resolve to the given `result` after the `ms` milliseconds.
   * If the client would be closed before the promise is resolved, the promise
   * will be rejected immediately with an Error.
   *
   * @param {number} ms - delay interval in milliseconds.
   * @param {any} [result] - the resolve argument.
   * @return {Promise}
  **/
  delay(ms, result) {
    var ts = this[timeout$];
    return new Promise((resolve, reject) => {
      var cancel = (err) => {
        clearTimeout(timeout);
        ts.delete(cancel);
        reject(err);
      },
      timeout = setTimeout(() => {
        ts.delete(cancel);
        resolve(result)
      }, ms);
      ts.add(cancel);
    });
  };


  /**
   * The last known cluster config.
   * @property peers {Map}
  **/

  /**
   * The last known cluster leader's ID.
   * @property leaderId {string|null}
  **/

  /**
   * Set the new leader.
   *
   * If the given `leaderId` is found in the cluster configuration only one connection to the
   * leader's url will be sustained. Otherwise the client will connect to all the known peers
   * in the cluster.
   *
   * @param {string|null} [leaderId] - the known leader ID or null.
   * @param {boolean} [forceSetUrls] - forces re-checking connections regardless if leader ID
   *                                   has changed or not.
   * @return {this}
  **/
  setLeader(leaderId, forceSetUrls) {
    const peers = this.peers;
    var url;
    if (forceSetUrls || this.leaderId !== leaderId) {
      if (leaderId) {
        url = peers.get(leaderId);
      }
      if (url) {
        if (this.leaderId !== leaderId) {
          debug('leader established: (%s) at %s', leaderId, url);
        }
        this.leaderId = leaderId;
        this.setUrls(url);
      }
      else {
        if (this.leaderId !== null) {
          debug('leader unknown');
        }
        this.leaderId = null;
        this.setUrls(Array.from(peers.values()));
      }
    }
    return this;
  }

  /**
   * Set the new cluster config.
   *
   * This call will update the last known cluster config and establish the connection
   * according to the given `leaderId` argument.
   *
   * See ZmqRaftClient.prototype.setLeader for details on how the connections are set up.
   *
   * @param {Array} [peers]
   * @param {string} [leaderId]
   * @return {this}
  **/
  setPeers(peers, leaderId) {
    if (peers !== undefined) {
      this.peers = parsePeers(peers);
    }
    return this.setLeader(leaderId, true);
  }

  /**
   * Invoke RequestConfig RPC.
   *
   * Resolves to {leaderId: string|null, urls: {id: url}} on success.
   *
   * The response will be accepted only from the current leader.
   *
   * On success this method will update the last known cluster configuration.
   *
   * @param {number} [timeout] - an interval in milliseconds, after which the request is
   *                             abandoned and the returned promise resolves with `undefined`.
   * @return {Promise}
   *
   * If the `timeout` is `0` or not specified the request will be retried indefinitely.
  **/
  requestConfig(timeout) {
    var expire;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;

    const request = () => {
      return super.requestConfig()
      .then(({leaderId, isLeader, peers}) => {
        /* always update peers */
        this.setPeers(peers, leaderId);
        if (isLeader) {
          if (expire === undefined) {
            /* clear pending on request forever only */
            this[rcPending$] = undefined;
          }
          var urls = {};
          peers.forEach(([id, url]) => (urls[id] = url));
          return {leaderId, urls};
        }
        else {
          /* expire now if must */
          if (expire !== undefined && now() >= expire)
            throw new TimeoutError();
          /* not a leader */
          return this.leaderId === null ? this.delay(this.serverElectionGraceDelay).then(request)
                                        : request();
        }
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured re-trying config request with another server');
          this.setLeader(null);
          return request();
        }
        else {
          if (expire === undefined) {
            /* clear pending on request forever only */
            this[rcPending$] = undefined;
          }
          throw err;
        }
      });
    };

    /* ensure only one request config pending forever */
    if (expire === undefined) {
      let pending = this[rcPending$];
      if (pending !== undefined) return pending;
      else return this[rcPending$] = request();
    }
    else return request();
  }

  /**
   * Invoke RequestLogInfo RPC.
   *
   * The returned promise resolves to an Object with the following properties:
   *
   *  - isLeader {boolean}
   *  - leaderId {string|null}
   *  - currentTerm {number}
   *  - firstIndex {number}
   *  - lastApplied {number}
   *  - commitIndex {number}
   *  - lastIndex {number}
   *  - snapshotSize {number}
   *  - pruneIndex {number}
   *
   *
   * @param {boolean} anyPeer - whether a response from any peer will be accepted.
   * @param {number} [timeout] - an interval in milliseconds, after which the request is
   *                             abandoned and the returned promise resolves with `undefined`.
   * @return {Promise}
   *
   * If the `timeout` is `0` or not specified the request will be retried indefinitely.
  **/
  requestLogInfo(anyPeer, timeout) {
    if (this.peers.size === 0) {
      return this.requestConfig(timeout).then(() => this.requestLogInfo(anyPeer, timeout));
    }

    var expire;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;

    const request = () => {
      return super.requestLogInfo()
      .then(info => {
        if (!anyPeer) {
          this.setLeader(info.leaderId);
          if (!info.isLeader) {
            /* expire now if must */
            if (expire !== undefined && now() >= expire)
              throw new TimeoutError();
            /* not a leader, re-try */
            return this.leaderId === null ? this.delay(this.serverElectionGraceDelay)
                                            .then(() => this.requestConfig(timeout)).then(request)
                                          : request();
          }
        }

        return info;
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured re-trying log info request with another server');
          this.setLeader(null);
          return request();
        }
        else throw err;
      });
    };

    return request();
  }

  /**
   * Invoke ConfigUpdate RPC.
   *
   * The returned promise resolves to the log index of the committed entry of Cold,new on success.
   *
   * `id` must be a 12 byte buffer or 24 byte unique hexadecimal string, following this:
   * https://docs.mongodb.com/manual/reference/method/ObjectId/ specification.
   * `id` should be freshly generated. Its "the seconds" part is important
   * because update requests might expire after `common.constants.DEFAULT_REQUEST_ID_TTL`
   * milliseconds.
   *
   * You may use `utils.id.genIdent()` function to generate them.
   *
   * @param {string|Buffer} id - a request ID to ensure idempotent updates.
   * @param {Array} peers - the new cluster configuration.
   * @param {number} [timeout] - an interval in milliseconds, after which the request is
   *                             abandoned and the returned promise is rejected with TimeoutError.
   * @return {Promise}
   *
   * If the `timeout` is `0` or not specified the request will be retried indefinitely.
  **/
  configUpdate(id, peers, timeout) {
    if (this.peers.size === 0) {
      return this.requestConfig(timeout).then(() => this.configUpdate(id, peers, timeout));
    }

    try {
      peers = encodeMsgPack( Array.from(parsePeers(peers)) );
    } catch(err) { return Promise.reject(err); }

    var expire;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;

    const request = () => {
      return super.configUpdate(id, peers)
      .catch(err => {
        if (err.isPeerNotLeader) {
          /* try the next server */
          this.setLeader(err.leaderId);
          /* expire now if must */
          if (expire !== undefined && now() >= expire)
            throw new TimeoutError();
          /* continue */
          return this.leaderId === null ? this.delay(this.serverElectionGraceDelay)
                                          .then(() => this.requestConfig(timeout)).then(request)
                                        : request();          
        }
        else if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured re-trying config update request with another server');
          this.setLeader(null);
          return request();
        }
        else throw err;
      });
    };

    return request();
  }

  /**
   * Invoke RequestUpdate RPC.
   *
   * The returned promise resolves to the log index of the committed entry.
   *
   * `id` must be a 12 byte buffer or 24 byte unique hexadecimal string, following this:
   * https://docs.mongodb.com/manual/reference/method/ObjectId/ specification.
   * `id` should be freshly generated. Its "the seconds" part is important
   * because update requests might expire after `common.constants.DEFAULT_REQUEST_ID_TTL`
   * milliseconds.
   *
   * You may use `utils.id.genIdent()` function to generate them.
   *
   * @param {string|Buffer} id - a request ID to ensure idempotent updates.
   * @param {Buffer} data - log entry data to modify the state.
   * @param {number} [timeout] - an interval in milliseconds, after which the request is
   *                             abandoned and the returned promise is rejected with TimeoutError.
   * @return {Promise}
   *
   * If the `timeout` is `0` or not specified the request will be retried indefinitely.
  **/
  requestUpdate(id, data, timeout) {
    if (this.peers.size === 0) {
      return this.requestConfig(timeout).then(() => this.requestUpdate(id, data, timeout));
    }
    var expire;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;
    const request = () => {
      return super.requestUpdate(id, data)
      .catch(err => {
        if (err.isPeerNotLeader) {
          /* try the next server */
          this.setLeader(err.leaderId);
          /* expire now if must */
          if (expire !== undefined && now() >= expire)
            throw new TimeoutError();
          /* continue */
          return this.leaderId === null ? this.delay(this.serverElectionGraceDelay)
                                          .then(() => this.requestConfig(timeout)).then(request)
                                        : request();
        }
        else if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured re-trying log update request with another server');
          this.setLeader(null);
          return request();
        }
        else throw err;
      });
    };

    return request();
  }

  /**
   * Perform RequestEntries RPC.
   *
   * Retrieve entries using the `receiver` callback.
   *
   * The received entries must be consumed ASAP and the retrieving of them can not be paused.
   * However it is possible to cancel the request while processing entries.
   *
   * Use ZmqRaftClient.prototype.requestEntriesStream to work with the back-pressured streams instead.
   *
   * The returned promise will resolve to `true` if all of the requested entries were received.
   * Otherwise the promise will resolve to `false` if the request was canceled by the receiver.
   *
   * The `receiver` function may modify its entries array argument (e.g. clear it).
   * The `receiver` may request to stop receiving entries if the function returns `false`
   * (that is an exact boolean `false`, not a falsy-ish value).
   *
   * The `receiver` function signature:
   *   (status: number, entries_or_snapshot_chunk: Array|Buffer, lastIndex: number,
   *    byteOffset?: number, snapshotSize?: number, isLastChunk?: boolean, snapshotTerm?: number
   *   ) => false|any
   *
   * The `status` in this case may be one of:
   *
   * - 1: this is the last batch.
   * - 2: expect more entries.
   * - 3: this is a snapshot chunk.
   *
   * @param {number} lastIndex - index of the log entry PRECEDING entries that are actually requested
   *                             (e.g. the index of the last received entry), specify 0 to start from 1.
   * @param {number} [count] - the maximum number of requested entries, only valid if > 0; otherwise ignored.
   * @param {Function} receiver - the function to process received entries.
   * @param {number} [timeout] - an interval in milliseconds, after which the request is considered as
   *                             expired and the returned promise may be rejected with TimeoutError.
   * @param {number} [snapshotOffset] - a hint for the server to start responding with snapshot chunks
   *           starting from this byte offset, providing the `lastIndex` precedes the actual snapshot index.
   * @return {Promise}
   *
   * If the `timeout` is `0` or not specified the request will be retried indefinitely.
   *
   * The expiration timeout is checked agains only if the server does not respond within the RPC
   * timeout interval, specified when the client was instantiated.
  **/
  requestEntries(lastIndex, count, receiver, timeout, snapshotOffset) {
    if (this.peers.size === 0) {
      return this.requestConfig(timeout)
                 .then(() => this.requestEntries(lastIndex, count, receiver, timeout));
    }
    if ('function' === typeof count) {
      snapshotOffset = timeout, timeout = receiver, receiver = count, count = undefined;
    }

    var expire
      , timeoutRPC = this.timeoutMs;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;
    const maxTimeoutRPC = timeoutRPC * 5
        , request = () => {
      return super.requestEntries(lastIndex, count, receiver, timeoutRPC, snapshotOffset)
      .catch(err => {
        var state = err.requestEntriesState;
        lastIndex      = state.lastIndex;
        count          = state.count;
        snapshotOffset = state.snapshotOffset;

        if (err.isPeerNotLeader) {
          /* try the next server */
          this.setLeader(err.leaderId);
          /* expire now if must */
          if (expire !== undefined && now() >= expire)
            throw new TimeoutError();
          /* continue */
          if (this.leaderId === null) { /* election in progress */
            timeoutRPC = this.timeoutMs; /* reset timeout */
            return this.delay(this.serverElectionGraceDelay)
                  .then(() => this.requestConfig(timeout)).then(request);
          }
          else return request();
        }
        else if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured re-trying entries request with another server');
          this.setLeader(null);
          /* increase timeout, to prevent streaming timeout loop */
          if ((timeoutRPC += this.timeoutMs) > maxTimeoutRPC) timeoutRPC = maxTimeoutRPC;
          return request();
        }
        else throw err;
      });
    };

    return request();
  }

  /**
   * Returns a Readable stream that retrieves entries via the RequestEntries RPC.
   *
   * The returned stream is lazy: RequestEntries RPC messages are sent to the cluster only when
   * data is requested via stream.Readable api.
   *
   * The returned stream can be back-pressured.
   *
   * The stream yields objects that are instances of either common.LogEntry or common.SnapshotChunk.
   *
   * `options`:
   *
   * - `count` {number} - the maximum number of requested entries, only valid if > 0; otherwise ignored.
   * - `timeout` {number} - an interval in milliseconds, after which the request is considered as
   *                        expired and the "error" event may be emitted with the TimeoutError argument.
   * - `snapshotOffset` {number} - a hint for the server to start responding with snapshot chunks starting
   *                  from this byte offset, providing the `lastIndex` precedes the actual snapshot index.
   *
   * Any other option is passed to the Readable constructor.
   *
   * @param {number} lastIndex - index of the log entry PRECEDING entries that are actually requested
   *                             (e.g. the index of the last received entry), specify 0 to start from 1.
   * @param {number|Object} [count|options]
   * @return {RequestEntriesStream}
  **/
  requestEntriesStream(lastIndex, options) {
    return new RequestEntriesStream(this, lastIndex, options);
  }

}
/*
class RequestEntriesStream extends Readable {
  constructor(client, lastIndex, options) {
    var count, timeout;
    if ('number' === typeof options) {
      count = options;
      options = undefined;
    }
    else if (options) {
      count = options.count;
      timeout = options.timeout;
    }
    count = +count;
    if (!isFinite(count) || count <= 0) count = undefined;
    var expire, currentByte = 0, prevIndex = lastIndex;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;

    super(Object.assign({}, options, {objectMode: true}));

    const queue = this._replyQueue = [];

    const msg = [requestEntriesTypeBuf, client[secretBuf$], lastIndex, count]
        , timeoutMs = client.timeoutMs
        , maxTimeoutMs = timeoutMs * 5;

    options = {
      timeout: timeoutMs,
      protocol: requestEntriesProtocol
    };

    options.onresponse = (res, reply, refresh) => {
      var status = res[0]
        , arg = res[1]
        , more = true;

      switch(status) {
      case RE_STATUS_NOT_LEADER:
        // try next server
        return arg;
      case RE_STATUS_MORE:
      case RE_STATUS_LAST:
      case RE_STATUS_SNAPSHOT:
        // copy lastIndex from response
        lastIndex = res[2];
        // handle snapshots
        if (status === RE_STATUS_SNAPSHOT) {
          status = RE_STATUS_MORE;

          let byteOffset = arg[0]
            , snapshotSize = arg[1]
            , snapshotTerm = arg[2]
            , chunk = res[3];
          if (currentByte !== byteOffset) throw new OutOfOrderError("ZmqRaftClient.requestEntries: snapshot chunks not in order");
          currentByte = byteOffset + chunk.length;
          chunk = bufferToSnapshotChunk(chunk, lastIndex, byteOffset, snapshotSize, snapshotTerm);
          more = this.push(chunk);

          // snapshot last chunk
          if (currentByte === snapshotSize) {
            msg[2] = lastIndex;
            if (count !== undefined) {
              if (count === 1) status = RE_STATUS_LAST;
              msg[3] = --count;
              msg.length = 4;
            }
            else {
              msg.length = 3;
            }
          }
          else {
            // not a last chunk
            if (count === undefined) msg[3] = null;
            msg[4] = currentByte;
          }
        }
        else {
          // handle entries
          let numEntries = res.length - 3;
          if (lastIndex - numEntries !== prevIndex) throw new OutOfOrderError("ZmqRaftClient.requestEntries: entries not in order");
          for(let i = 1; i <= numEntries; ++i) {
            more = this.push(
              bufferToLogEntry(res[i + 2], prevIndex + i)
            );
          }
          msg[2] = lastIndex;
          if (count !== undefined) msg[3] = (count -= numEntries);
        }

        prevIndex = lastIndex;

        if (status === RE_STATUS_MORE) {
          // request more or ask to stop
          if (count <= 0) {
            // set count = 0 to stop streaming from the server and finish
            msg[3] = 0;
            if (queue.length !== 0) this._consumeReplyQueue();
            reply(msg);
            // finish request
            return true;
          } else if (more) {
            // reply asking for more
            if (queue.length !== 0) this._consumeReplyQueue();
            refresh();
            reply(msg);
          } else {
            // back pressured
            // prevent request timeout
            refresh(0);
            let msgclone = msg.slice(0);
            // postpone replying
            queue.push(() => {
              refresh(options.timeout);
              reply(msgclone);
            });
          }
        }
        else {
          // last response: finish request
          if (queue.length !== 0) this._consumeReplyQueue();
          return true;
        }

        break;

      default:
        throw new Error("unknown status");
      }
    };

    const request = () => client.request(msg, options)
      .then(result => {
        // all received
        if (result === true) {
          this.push(null);
          return;
        }
        // not a leader
        client.setLeader(result);
        // expire now if must
        if (expire !== undefined && now() >= expire)
          throw new TimeoutError();
        // continue
        if (client.leaderId === null) { // election in progress
          options.timeout = timeoutMs; // reset timeout
          return client.delay(client.serverElectionGraceDelay)
                .then(() => client.requestConfig(timeout)).then(request);
        }
        else return request();
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured trying to find another server');
          client.setLeader(null);
          // increase timeout, to prevent streaming timeout loop
          if ((options.timeout += timeoutMs) > maxTimeoutMs) options.timeout = maxTimeoutMs;
          return request();
        }
        else throw err;
      })
      .catch(err => this.emit('error', err));

    queue.push(request);
  }

  _read(size) {
    try {
      this._consumeReplyQueue();
    } catch(err) {
      setImmediate(() => this.emit('error', err));
    }
  }

  _consumeReplyQueue() {
    var queue = this._replyQueue;
    for(let i = 0, len = queue.length; i < len; ++i) {
      queue[i]();
    }
    queue.length = 0;
  }
}
*/
// ZmqRaftClient.OutOfOrderError = OutOfOrderError;
ZmqRaftClient.ZmqRaftClient = ZmqRaftClient;
ZmqRaftClient.TimeoutError = TimeoutError;
ZmqRaftClient.RequestEntriesStream = RequestEntriesStream;
module.exports = exports = ZmqRaftClient;
