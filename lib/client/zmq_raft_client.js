/* 
 *  Copyright (c) 2016-2020 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray
    , now     = Date.now;

const assert = require('assert');

const { encode: encodeMsgPack } = require('msgpack-lite');

const { assertConstantsDefined, parsePeers } = require('../utils/helpers');

/* expect response within this timeout, if not will try the next server */
const { SERVER_ELECTION_GRACE_DELAY
      } = require('../common/constants');

assertConstantsDefined({
  SERVER_ELECTION_GRACE_DELAY
}, 'number');

const ZmqRaftPeerClient = require('../client/zmq_raft_peer_client');
const TimeoutError = ZmqRaftPeerClient.TimeoutError;
const RequestEntriesStream = ZmqRaftPeerClient.RequestEntriesStream;

const rcPending$ = Symbol('rcPending');

const debug = require('debug')('zmq-raft:client');

/**
 * This client can be used to execute the 0MQ Raft RPC protocol on the cluster of peers.
 * 
 * Overrides the ZmqRaftPeerClient RPC methods making them resilient to peer failures.
 * 
 * The meaning of the `rpctimeout` argument of the overridden RPC methods should be understood
 * as follows. Promises returned from ZmqRaftPeerClient methods would reject with a timeout error
 * while waiting for a single response from the peer, whereas the RPC methods of ZmqRaftClient
 * will repeat the request to every known peer in the cluster in this instance.
 * The `rpctimeout` argument given to these RPC methods is an expiration timeout put on top of
 * the whole RPC process. The TimeoutError will be thrown possibly after several attempts to
 * contact several different peers.
 * 
 * To control the peer response timeout, after which the request will be reattempted to the next
 * peer in the cluster, use the `timeout` option while initializing a new instance of ZmqRaftClient.
 * 
 * RPC methods of ZmqRaftClient never throw PeerNotLeaderError. Instead, they retry sending
 * requests to the new leader. When the election is still pending, a client gives a little time
 * (`serverElectionGraceDelay`) for the cluster to elect the new leader before reattempting the
 * pending request.
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
   *                    this option has precedence over `urls`, and if provided, the peer list
   *                    is not being initially fetched via Request Config RPC.
   * - `secret` {string|Buffer}: A cluster identifying string which is sent and verified against
   *                             in each message.
   * - `timeout` {int32}: A time interval in milliseconds after which we consider a server peer
   *                      to be unresponsive; the request will be reattempted if waiting for the
   *                      response  exceeds this interval; in this instance the next request 
   *                      attempt will be send to the next peer in the cluster.
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
    return super.close();
  }

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
   * Set the new cluster configuration.
   * 
   * This call will update the last known cluster configuration and establish the connection
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
   * The returned promise resolves to an Object with the following properties on success:
   * - `leaderId` {string} - ID of the current LEADER and the responding peer.
   * - `isLeader` {boolean} - indicates the LEADER state of the responding peer, always `true`.
   * - `peers` {Array<[id, url]>} - the current cluster configuration.
   * - `urls`: {{id: url}} - same as `peers`, but provided as an Object map for convenience.
   * 
   * The response will be authoritative - is accepted only from the current leader.
   * 
   * On success this method will update the last known cluster configuration.
   * 
   * If `rpctimeout` argument is 0, null or undefined, the request will be reattempted indefinitely.
   * 
   * The promise will be rejected with TimeoutError only when both conditions are met:
   * - Waiting for the response from the last peer takes longer than the `timeout` option given to
   *   the constructor, and
   * - the request has expired.
   * 
   * @param {int32} [rpctimeout] - the time, in milliseconds, until the request expires.
   * @return {Promise}
  **/
  requestConfig(rpctimeout) {
    var expire;
    rpctimeout |= 0;
    if (rpctimeout !== 0) expire = now() + rpctimeout;

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
          let urls = {};
          peers.forEach(([id, url]) => { urls[id] = url; });
          return {leaderId, isLeader, peers, urls};
        }
        else {
          /* expire now? */
          if (expire !== undefined && now() >= expire)
            throw new TimeoutError();
          /* not a leader */
          return this.leaderId === null ? this.delay(this.serverElectionGraceDelay).then(request)
                                        : request();
        }
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('response timeout: reattempting config request with another peer');
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
   * The promise will be rejected with TimeoutError only when both conditions are met:
   * - Waiting for the response from the last peer takes longer than the `timeout` option given to
   *   the constructor, and
   * - the request has expired.
   * 
   * @param {boolean} anyPeer - whether a response from any peer will be accepted.
   * @param {int32} [rpctimeout] - the time, in milliseconds, until the request expires.
   * @return {Promise}
  **/
  requestLogInfo(anyPeer, rpctimeout) {
    if (this.peers.size === 0) {
      return this.requestConfig(rpctimeout).then(() => this.requestLogInfo(anyPeer, rpctimeout));
    }

    var expire;
    rpctimeout |= 0;
    if (rpctimeout !== 0) expire = now() + rpctimeout;

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
                                            .then(() => this.requestConfig(rpctimeout))
                                            .then(request)
                                          : request();
          }
        }

        return info;
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('response timeout: reattempting log info request with another peer');
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
   * The promise will be rejected with TimeoutError only when both conditions are met:
   * - Waiting for the response from the last peer takes longer than the `timeout` option given to
   *   the constructor, and
   * - the request has expired.
   * 
   * @param {string|Buffer} id - a request ID to ensure idempotent updates.
   * @param {Array} peers - the new cluster configuration.
   * @param {int32} [rpctimeout] - the time, in milliseconds, until the request expires.
   * @return {Promise}
  **/
  configUpdate(id, peers, rpctimeout) {
    if (this.peers.size === 0) {
      return this.requestConfig(rpctimeout).then(() => this.configUpdate(id, peers, rpctimeout));
    }

    try {
      peers = encodeMsgPack( Array.from(parsePeers(peers)) );
    } catch(err) { return Promise.reject(err); }

    var expire;
    rpctimeout |= 0;
    if (rpctimeout !== 0) expire = now() + rpctimeout;

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
                                          .then(() => this.requestConfig(rpctimeout))
                                          .then(request)
                                        : request();
        }
        else if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('response timeout: reattempting update config request with another peer');
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
   * The promise will be rejected with TimeoutError only when both conditions are met:
   * - Waiting for the response from the last peer takes longer than the `timeout` option given to
   *   the constructor, and
   * - the request has expired.
   * 
   * @param {string|Buffer} id - a request ID to ensure idempotent updates.
   * @param {Buffer} data - log entry data to modify the state.
   * @param {int32} [rpctimeout] - the time, in milliseconds, until the request expires.
   * @return {Promise}
  **/
  requestUpdate(id, data, rpctimeout) {
    if (this.peers.size === 0) {
      return this.requestConfig(rpctimeout).then(() => this.requestUpdate(id, data, rpctimeout));
    }
    var expire;
    rpctimeout |= 0;
    if (rpctimeout !== 0) expire = now() + rpctimeout;
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
                                          .then(() => this.requestConfig(rpctimeout)).then(request)
                                        : request();
        }
        else if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('response timeout: reattempting update state request with another peer');
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
   * Retrieve RAFT log entries using the `receiver` callback.
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
   * The promise will be rejected with TimeoutError only when both conditions are met:
   * - Waiting for the last response from the server takes longer than the `timeout` option given to
   *   the constructor, and
   * - the request has expired.
   * 
   * @param {number} lastIndex - index of the log entry PRECEDING entries that are actually requested
   *                             (e.g. the index of the last received entry), specify 0 to start from 1.
   * @param {number} [count] - the maximum number of requested entries, only valid if > 0; otherwise ignored.
   * @param {Function} receiver - the function to process received entries.
   * @param {int32} [rpctimeout] - the time, in milliseconds, until the request expires.
   * @param {number} [snapshotOffset] - a hint for the server to start responding with snapshot chunks
   *        starting from this byte offset, providing the `lastIndex` precedes the actual snapshot index.
   * @return {Promise}
  **/
  requestEntries(lastIndex, count, receiver, rpctimeout, snapshotOffset) {
    if (this.peers.size === 0) {
      return this.requestConfig(rpctimeout)
                 .then(() => this.requestEntries(lastIndex, count, receiver, rpctimeout));
    }
    if ('function' === typeof count) {
      snapshotOffset = rpctimeout, rpctimeout = receiver, receiver = count, count = undefined;
    }

    var expire
      , peerTimeout = this.timeoutMs;
    rpctimeout |= 0;
    if (rpctimeout !== 0) expire = now() + rpctimeout;
    const maxPeerTimeout = peerTimeout * 5
        , request = () => {
      return super.requestEntries(lastIndex, count, receiver, peerTimeout, snapshotOffset)
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
            peerTimeout = this.timeoutMs; /* reset timeout */
            return this.delay(this.serverElectionGraceDelay)
                  .then(() => this.requestConfig(rpctimeout)).then(request);
          }
          else return request();
        }
        else if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('response timeout: reattempting log entries request with another peer');
          this.setLeader(null);
          /* increase peer timeout, to prevent streaming peer timeout loop */
          if ((peerTimeout += this.timeoutMs) > maxPeerTimeout) peerTimeout = maxPeerTimeout;
          return request();
        }
        else throw err;
      });
    };

    return request();
  }

  /**
   * Returns a Readable stream that retrieves RAFT log entries via the RequestEntries RPC.
   * 
   * The returned stream is lazy: RequestEntries RPC messages are sent to the cluster only when
   * data is requested via stream.Readable api.
   * 
   * The returned stream can be back-pressured.
   * 
   * The stream yields objects that are instances of either common.LogEntry or common.SnapshotChunk.
   * 
   * The stream will emit the "error" event with the TimeoutError only when both conditions are met:
   * - Waiting for the last response from the server takes longer than the `timeout` option given to
   *   the constructor, and
   * - the request has expired.
   *
   * `options`:
   * 
   * - `count` {number} - the maximum number of requested entries, only valid if > 0; otherwise ignored.
   * - `rpctimeout` {int32} - the time, in milliseconds, until the request expires.
   * - `snapshotOffset` {number} - a hint for the server to start responding with snapshot chunks starting
   *                  from this byte offset, providing the `lastIndex` precedes the actual snapshot index.
   * 
   * Any other option is passed to the Readable constructor.
   * 
   * @param {number} lastIndex - index of the log entry PRECEDING entries that are actually requested
   *                             (e.g. the index of the last received entry), specify 0 to start from 1.
   * @param {Object|number} [options|count]
   * @return {RequestEntriesStream}
  **/
  requestEntriesStream(lastIndex, options) {
    if ('number' !== typeof options) {
      options = Object.assign({}, options);
      if (options.rpctimeout !== undefined) {
        options.timeout = options.rpctimeout;
      }
    }
    return new RequestEntriesStream(this, lastIndex, options);
  }

}

ZmqRaftClient.ZmqRaftClient = ZmqRaftClient;
module.exports = exports = ZmqRaftClient;
