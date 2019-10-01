/* 
 *  Copyright (c) 2016-2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray
    , now     = Date.now;

const assert = require('assert');

const { Readable } = require('stream');

const { encode: encodeMsgPack } = require('msgpack-lite');

const { assertConstantsDefined, parsePeers } = require('../utils/helpers');
const { bufferToLogEntry } = require('../common/log_entry');
const { bufferToSnapshotChunk } = require('../common/snapshot_chunk');

/* expect response within this timeout, if not will try the next server */
const { SERVER_RESPONSE_TIMEOUT
      , SERVER_ELECTION_GRACE_DELAY

      , RE_STATUS_NOT_LEADER
      , RE_STATUS_LAST
      , RE_STATUS_MORE
      , RE_STATUS_SNAPSHOT

      , REQUEST_CONFIG
      , REQUEST_UPDATE
      , CONFIG_UPDATE
      , REQUEST_ENTRIES
      , REQUEST_LOG_INFO
      } = require('../common/constants');

assertConstantsDefined({
  SERVER_RESPONSE_TIMEOUT
, SERVER_ELECTION_GRACE_DELAY
, RE_STATUS_NOT_LEADER
, RE_STATUS_LAST
, RE_STATUS_MORE
, RE_STATUS_SNAPSHOT
}, 'number');

assertConstantsDefined({
  REQUEST_CONFIG
, REQUEST_UPDATE
, CONFIG_UPDATE
, REQUEST_ENTRIES
, REQUEST_LOG_INFO
}, 'string', true);

const requestConfigTypeBuf  = Buffer.from(REQUEST_CONFIG);
const requestUpdateTypeBuf  = Buffer.from(REQUEST_UPDATE);
const configUpdateTypeBuf   = Buffer.from(CONFIG_UPDATE);
const requestEntriesTypeBuf = Buffer.from(REQUEST_ENTRIES);
const requestLogInfoTypeBuf = Buffer.from(REQUEST_LOG_INFO);

const { createFramesProtocol } = require('../protocol');

const requestConfigProtocol = createFramesProtocol('RequestConfig');
const requestUpdateProtocol = createFramesProtocol('RequestUpdate');
const configUpdateProtocol = createFramesProtocol('ConfigUpdate');
const requestEntriesProtocol = createFramesProtocol('RequestEntries');
const requestLogInfoProtocol = createFramesProtocol('RequestLogInfo');

const ZmqProtocolSocket = require('../client/zmq_protocol_socket');

const TimeoutError = ZmqProtocolSocket.TimeoutError;

const secretBuf$ = Symbol('secretBuf');
const rcPending$ = Symbol('rcPending');
const timeout$   = Symbol('timeout');

const debug = require('debug')('zmq-raft:client');

function OutOfOrderError(message) {
  Error.captureStackTrace(this, OutOfOrderError);
  this.name = 'OutOfOrderError';
  this.message = message || 'chunks received out of order';
}

OutOfOrderError.prototype = Object.create(Error.prototype);
OutOfOrderError.prototype.constructor = OutOfOrderError;
OutOfOrderError.prototype.isOutOfOrder = true;


class ZmqRaftClient extends ZmqProtocolSocket {

  /**
   * Create ZmqRaftClient
   *
   * `options` may be one of:
   *
   * - `url` {String}: A seed url to fetch peer urls from via a Request Config RPC.
   * - `urls` {Array}: An array of seed urls to fetch peers from via a Request Config RPC.
   * - `peers` {Array}: An array of established zmq raft server descriptors;
   *                    `peers` has precedence over `urls` and if provided the peer list
   *                    is not being fetched via Request Config RPC.
   * - `secret` {String|Buffer}: A cluster identifying string which is sent and verified against
   *                             in each message.
   * - `timeout` {number}: A time in milliseconds after which we consider a server peer as unresponsive.
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
   * - `serverElectionGraceDelay` {number}: A delay in milliseconds to wait for the Raft peers
   *                                     to elect a new leader before retrying (default: 300).
   *
   * @param {string|Array} [urls]
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
    var options = Object.assign({timeout: SERVER_RESPONSE_TIMEOUT}, options);

    super(urls, options);

    this.peers = peers;
    this.leaderId = null;
    this[secretBuf$] = Buffer.from(options.secret || '');
    this[timeout$] = new Set();
    this.serverElectionGraceDelay = (options.serverElectionGraceDelay|0) || SERVER_ELECTION_GRACE_DELAY;
    this.heartbeatMs = options.heartbeat|0;
    if (this.heartbeatMs > 0) {// this[Symbol.for('connected')]
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
   * Disconnect, close socket and reject all pending requests
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
   * Creates cancellable time delay promise
   *
   * @param {number} ms - delay interval
   * @param {*} [result] - resolve argument
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
   * @property peers {Map}
  **/

  /**
   * @property leaderId {string|null}
  **/

  /**
   * Set new leader
   *
   * will reconnect if necessary
   *
   * @param {string} [leaderId]
   * @param {bool} [forceSetUrls]
   * @return {Promise}
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
   * Set server peers and connect to the new ones and disconnect from the old ones
   *
   * @param {Array} [peers]
   * @param {string} [leaderId]
   * @return {Promise}
  **/
  setPeers(peers, leaderId) {
    if (peers !== undefined) {
      this.peers = parsePeers(peers);
    }
    return this.setLeader(leaderId, true);
  }

  /**
   * Send request config rpc
   *
   * Resolves to {leaderId{string|null}, urls{Object}}
   *
   * On success it will update internal peer urls
   *
   * By default it will repeat requests forever until resolved or errored
   *
   * @param {number} [timeout] in case there is a problem with reaching a peer after this timeout
   *                           request is abandoned (rejected with TimeoutError)
   * @return {Promise}
  **/
  requestConfig(timeout) {
    var expire;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;
    const msg = [requestConfigTypeBuf, this[secretBuf$]]
        , options = {protocol: requestConfigProtocol}
        , request = () => {
      return this.request(msg, options)
      .then(([isLeader, leaderId, peers]) => {
        /* always update peers */
        this.setPeers(peers, leaderId);
        if (isLeader) {
          this[rcPending$] = undefined;
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
          debug('timeout occured trying to find another server');
          this.setLeader(null);
          return request();
        }
        else {
          this[rcPending$] = undefined;
          throw err;
        }
      });
    };

    /* ensure only one request config pending forever */
    var pending = this[rcPending$];
    if (expire === undefined) {
      if (pending !== undefined) return pending;
      else return this[rcPending$] = request();
    }
    else return request();
  }

  /**
   * Send request log info rpc
   *
   * Returned promise resolves to Object with the following properties:
   *
   *  - isLeader {bool}
   *  - leaderId {string|null}
   *  - currentTerm {number}
   *  - firstIndex {number}
   *  - lastApplied {number}
   *  - commitIndex {number}
   *  - lastIndex {number}
   *  - snapshotSize {number}
   *  - pruneIndex {number}
   *
   * By default it will repeat requests forever until resolved or errored
   *
   * @param {bool} anyPeer response from not a leader is ok
   * @param {number} [timeout] in case there is a problem with reaching a peer after this timeout
   *                           request is abandoned (rejected with TimeoutError)
   * @return {Promise}
  **/
  requestLogInfo(anyPeer, timeout) {
    if (this.peers.size === 0) {
      return this.requestConfig(timeout).then(() => this.requestLogInfo(anyPeer, timeout));
    }
    var expire;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;
    const msg = [requestLogInfoTypeBuf, this[secretBuf$]]
        , options = {protocol: requestLogInfoProtocol}
        , request = () => {
      return this.request(msg, options)
      .then(res => {
        if (!anyPeer && !res[0]) {
          /* not a leader, re-try */
          this.setLeader(res[1]);
          /* expire now if must */
          if (expire !== undefined && now() >= expire)
            throw new TimeoutError();
          return this.leaderId === null ? this.delay(this.serverElectionGraceDelay)
                                          .then(() => this.requestConfig(timeout)).then(request)
                                        : request();
        }
        var [
          isLeader,
          leaderId,
          currentTerm,
          firstIndex,
          lastApplied,
          commitIndex,
          lastIndex,
          snapshotSize,
          pruneIndex
        ] = res;

        if (!anyPeer) this.setLeader(leaderId);

        return {
          isLeader,
          leaderId,
          currentTerm,
          firstIndex,
          lastApplied,
          commitIndex,
          lastIndex,
          snapshotSize,
          pruneIndex
        };
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured trying to find another server');
          this.setLeader(null);
          return request();
        }
        else throw err;
      });
    };

    return request();
  }

  /**
   * Send config update rpc
   *
   * On success it will resolve to log index of the commited entry of Cold,new
   *
   * By default it will repeat requests (perhaps to many peers) until resolved
   * or errored (expired).
   *
   * `id` must be a 12 byte buffer or 24 byte unique hexadecimal string, following this:
   * https://docs.mongodb.com/manual/reference/method/ObjectId/ specification.
   * `id` should be freshly generated. Its "the seconds" part is important
   * because update requests might expire after `common.constants.DEFAULT_REQUEST_ID_TTL`
   * milliseconds.
   *
   * You may use `utils.id.genIdent()` function to generate them.
   *
   * @param {string|Buffer} id - update request id to ensure idempotent updates
   * @param {Array} peers - new cluster configuration
   * @param {number} [timeout] in case there is a problem with reaching a leader after this timeout
   *                           request is abandoned (rejected with TimeoutError)
   * @return {Promise}
  **/
  configUpdate(id, peers, timeout) {
    if (id === undefined) return Promise.reject(new Error("configUpdate: required id argument is missing"));
    if (this.peers.size === 0) {
      return this.requestConfig(timeout).then(() => this.configUpdate(id, peers, timeout));
    }
    try {
      peers = Array.from(parsePeers(peers));
    } catch(err) { return Promise.reject(err); }
    var expire;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;
    const msg = [configUpdateTypeBuf, this[secretBuf$], encodeMsgPack(peers)]
        , options = {
            id: id,
            protocol: configUpdateProtocol,
            onresponse: (res, reply, refresh) => {
              /* response that update was accepted */
              if (res[0] === 1 && res[1] === undefined) {
                debug('accepted update');
                refresh();
              }
              else return res;
            }
          }
        , request = () => {
      return this.request(msg, options)
      .then(res => {
        var status = res[0]
          , arg = res[1];
        /* response that update was commited */
        if (status === 1) {
          return arg;
        }
        /* configuration error */
        else if (status === 2) {
          throw new Error(arg.message);
        }
        /* cluster in transition */
        else if (status === 3) {
          throw new Error("configUpdate: cluster in configuration transition");
        }
        /* request id get too old */
        else if (status !== 0) {
          throw new Error("configUpdate: expired request id");
        }
        /* try the next server */
        this.setLeader(arg);
        /* expire now if must */
        if (expire !== undefined && now() >= expire)
          throw new TimeoutError();
        /* continue */
        return this.leaderId === null ? this.delay(this.serverElectionGraceDelay)
                                        .then(() => this.requestConfig(timeout)).then(request)
                                      : request();
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured trying to find another server');
          this.setLeader(null);
          return request();
        }
        else throw err;
      });
    };

    return request();
  }

  /**
   * Send request update rpc
   *
   * On success it will resolve to a log index of the commited and applied entry
   *
   * By default it will repeat requests (perhaps to many peers) until resolved
   * or errored (expired).
   *
   * `id` must be a 12 byte buffer or 24 byte unique hexadecimal string, following this:
   * https://docs.mongodb.com/manual/reference/method/ObjectId/ specification.
   * `id` should be freshly generated. Its "the seconds" part is important
   * because update requests might expire after `common.constants.DEFAULT_REQUEST_ID_TTL`
   * milliseconds.
   *
   * You may use `utils.id.genIdent()` function to generate them.
   *
   * @param {string|Buffer} id - update request id to ensure idempotent updates
   * @param {Buffer} data to append to the log
   * @param {number} [timeout] in case there is a problem with reaching a leader after this timeout
   *                           request is abandoned (rejected with TimeoutError)
   * @return {Promise}
  **/
  requestUpdate(id, data, timeout) {
    if (id === undefined) return Promise.reject(new Error("requestUpdate: required id argument is missing"));
    if (this.peers.size === 0) {
      return this.requestConfig(timeout).then(() => this.requestUpdate(id, data, timeout));
    }
    var expire;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;
    const msg = [requestUpdateTypeBuf, this[secretBuf$], data]
        , options = {
            id: id,
            protocol: requestUpdateProtocol,
            onresponse: (res, reply, refresh) => {
              /* response that update was accepted */
              if (res[0] === true && res[1] === undefined) {
                debug('accepted update');
                refresh();
              }
              else return res;
            }
          }
        , request = () => {
      return this.request(msg, options)
      .then(res => {
        var arg = res[1];
        /* response that update was commited */
        if (res[0] === true) {
          return arg;
        }
        /* request id get too old */
        else if (arg === undefined) {
          throw new Error("requestUpdate: expired request id");
        }
        /* try the next server */
        this.setLeader(arg);
        /* expire now if must */
        if (expire !== undefined && now() >= expire)
          throw new TimeoutError();
        /* continue */
        return this.leaderId === null ? this.delay(this.serverElectionGraceDelay)
                                        .then(() => this.requestConfig(timeout)).then(request)
                                      : request();
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured trying to find another server');
          this.setLeader(null);
          return request();
        }
        else throw err;
      });
    };

    return request();
  }

  /**
   * Perform request entries rpc
   *
   * Retrieve entries using `receiver` callback.
   *
   * On success it will resolve to true or false if canceled by the receiver.
   *
   * By default it will repeat requests to all the known peers forever until resolved or errored.
   *
   * `receiver` may modify entries array (e.g. clear it if has processed them).
   * Stops receiving entries if the receiver returns false (exactly false, not falsyish).
   *
   * `receiver` signature:
   * (status, entries or chunk{Array|Buffer}, lastIndex{number} [,
   *   byteOffset{number}, snapshotSize{number}, isLastChunk{bool}, snapshotTerm{number}]) => bool
   *
   * @param {number} lastIndex - index after which you need entries (last index of already received entry)
   * @param {number} [count]
   * @param {Function} receiver
   * @param {number} [timeout] in case there is a problem with reaching a leader after this timeout
   *                           request is abandoned (rejected with TimeoutError)
   * @return {Promise}
  **/
  requestEntries(lastIndex, count, receiver, timeout) {
    if (this.peers.size === 0) {
      return this.requestConfig(timeout).then(() => this.requestEntries(lastIndex, count, receiver, timeout));
    }
    if ('function' === typeof count) {
      timeout = receiver, receiver = count, count = undefined;
    }
    else {
      count = +count;
      if (!isFinite(count) || count <= 0) count = undefined;
      if ('function' !== typeof receiver) return Promise.reject('receiver must be a function');
    }
    var expire, currentByte = 0, prevIndex = lastIndex;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;
    const msg = [requestEntriesTypeBuf, this[secretBuf$], lastIndex, count]
        , timeoutMs = this.timeoutMs
        , maxTimeoutMs = timeoutMs * 5
        , options = {
            timeout: timeoutMs,
            protocol: requestEntriesProtocol,
            onresponse: (res, reply, refresh) => {
              var status = res[0]
                , arg = res[1]
                , isLastChunk
                , byteOffset, snapshotSize, snapshotTerm, chunk
                , numEntries
                , ret;

              switch(status) {
              case RE_STATUS_NOT_LEADER:
                /* try next server */
                this.setLeader(arg);
                return null;
              case RE_STATUS_MORE:
              case RE_STATUS_LAST:
              case RE_STATUS_SNAPSHOT:
                /* copy lastIndex from response */
                lastIndex = res[2];
                /* push entries to result */
                if (status === RE_STATUS_SNAPSHOT) {
                  status = RE_STATUS_MORE;
                  byteOffset = arg[0];
                  snapshotSize = arg[1];
                  snapshotTerm = arg[2];
                  chunk = res[3];
                  if (currentByte !== byteOffset) throw new OutOfOrderError("ZmqRaftClient.requestEntries: snapshot chunks not in order");
                  currentByte = byteOffset + chunk.length;
                  isLastChunk = currentByte === snapshotSize;
                  ret = receiver(status, chunk, lastIndex, byteOffset, snapshotSize, isLastChunk, snapshotTerm);
                  if (isLastChunk) {
                    msg[2] = lastIndex;

                    if (count !== undefined) {
                      if (count === 1) status = RE_STATUS_LAST;
                      msg[3] = --count;
                    }
                  }
                }
                else {
                  msg[2] = lastIndex;
                  res.splice(0, 3);
                  numEntries = res.length;
                  if (lastIndex - numEntries !== prevIndex) throw new OutOfOrderError("ZmqRaftClient.requestEntries: entries not in order");
                  ret = receiver(status, res, lastIndex);
                  if (count !== undefined) msg[3] = (count -= numEntries);
                }

                prevIndex = lastIndex;

                if (status === RE_STATUS_MORE) {
                  /* request more or ask to stop */
                  if (ret === false || count <= 0) {
                    status = RE_STATUS_LAST;
                    /* set count = 0 to stop streaming from the server */
                    msg[3] = 0;
                  } else refresh();
                  reply(msg);
                }

                if (status === RE_STATUS_LAST) return ret !== false;

                break;

              default:
                throw new Error("unknown status");
              }
            }
          }
        , request = () => {
      return this.request(msg, options)
      .then(result => {
        /* all received */
        if (result !== null) {
          return result;
        }
        /* expire now if must */
        if (expire !== undefined && now() >= expire)
          throw new TimeoutError();
        /* continue */
        if (this.leaderId === null) { /* election in progress */
          options.timeout = timeoutMs; /* reset timeout */
          return this.delay(this.serverElectionGraceDelay)
                .then(() => this.requestConfig(timeout)).then(request);
        }
        else return request();
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured trying to find another server');
          this.setLeader(null);
          /* increase timeout, to prevent streaming timeout loop */
          if ((options.timeout += timeoutMs) > maxTimeoutMs) options.timeout = maxTimeoutMs;
          return request();
        }
        else throw err;
      });
    };

    return request();
  }

  /**
   * Returns request entries rpc readable stream
   *
   * The returned stream is lazy and can be back-pressured.
   * RequestEntries RPC are sent to the cluster only when data is requested via stream.Readable api.
   *
   * The data provided by the stream is either common.LogEntry or common.SnapshotChunk instances.
   *
   * By default it will repeat requests to all the known peers forever until resolved or errored.
   *
   * options:
   *
   * - `count` {number} - maximum number of requested entries
   * - `timeout` {number} - in case there is a problem with reaching a leader after this timeout
   *                        request is abandoned (error event is emited on stream with TimeoutError argument)
   *
   * @param {number} lastIndex - index after which you need entries (last index of already received entry)
   * @param {number|Object} [count|options]
   * @return {Readable}
  **/
  requestEntriesStream(lastIndex, options) {
    return new RequestEntriesStream(this, lastIndex, options);
  }

}

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
        /* try next server */
        client.setLeader(arg);
        return null;
      case RE_STATUS_MORE:
      case RE_STATUS_LAST:
      case RE_STATUS_SNAPSHOT:
        /* copy lastIndex from response */
        lastIndex = res[2];
        /* handle snapshots */
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

          /* snapshot last chunk */
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
            /* not a last chunk */
            if (count === undefined) msg[3] = null;
            msg[4] = currentByte;
          }
        }
        else {
          /* handle entries */
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
          /* request more or ask to stop */
          if (count <= 0) {
            /* set count = 0 to stop streaming from the server and finish */
            msg[3] = 0;
            if (queue.length !== 0) this._consumeReplyQueue();
            reply(msg);
            /* finish request */
            return true;
          } else if (more) {
            /* reply asking for more */
            if (queue.length !== 0) this._consumeReplyQueue();
            refresh();
            reply(msg);
          } else {
            /* back pressured */
            /* prevent request timeout */
            refresh(0);
            let msgclone = msg.slice(0);
            /* postpone replying */
            queue.push(() => {
              refresh(options.timeout);
              reply(msgclone);
            });
          }
        }
        else {
          /* last response: finish request */
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
        /* all received */
        if (result !== null) {
          this.push(null);
          return;
        }
        /* expire now if must */
        if (expire !== undefined && now() >= expire)
          throw new TimeoutError();
        /* continue */
        if (client.leaderId === null) { /* election in progress */
          options.timeout = timeoutMs; /* reset timeout */
          return client.delay(client.serverElectionGraceDelay)
                .then(() => client.requestConfig(timeout)).then(request);
        }
        else return request();
      })
      .catch(err => {
        if (err.isTimeout && (expire === undefined || now() < expire)) {
          debug('timeout occured trying to find another server');
          client.setLeader(null);
          /* increase timeout, to prevent streaming timeout loop */
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

ZmqRaftClient.OutOfOrderError = OutOfOrderError;
ZmqRaftClient.ZmqRaftClient = ZmqRaftClient;
ZmqRaftClient.TimeoutError = TimeoutError;
ZmqRaftClient.RequestEntriesStream = RequestEntriesStream;
module.exports = exports = ZmqRaftClient;
