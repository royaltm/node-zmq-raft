/* 
 *  Copyright (c) 2020 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isArray = Array.isArray;

const assert = require('assert');

const { Readable } = require('stream');

const { encode: encodeMsgPack } = require('msgpack-lite');

const { assertConstantsDefined, parsePeers } = require('../utils/helpers');
const { bufferToLogEntry } = require('../common/log_entry');
const { bufferToSnapshotChunk } = require('../common/snapshot_chunk');

const { SERVER_RESPONSE_TIMEOUT

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
const timeout$   = Symbol('timeout');
const request$ = Symbol('request');

const debug = require('debug')('zmq-raft:peer-client');

function PeerNotLeaderError(leaderId) {
  Error.captureStackTrace(this, PeerNotLeaderError);
  this.name = 'PeerNotLeaderError';
  this.message = 'the responding peer is currently not the LEADER of its cluster';
  this.leaderId = leaderId || null;
}

PeerNotLeaderError.prototype = Object.create(Error.prototype);
PeerNotLeaderError.prototype.constructor = PeerNotLeaderError;
PeerNotLeaderError.prototype.isPeerNotLeader = true;

function OutOfOrderError(message) {
  Error.captureStackTrace(this, OutOfOrderError);
  this.name = 'OutOfOrderError';
  this.message = message || 'chunks received out of order';
}

OutOfOrderError.prototype = Object.create(Error.prototype);
OutOfOrderError.prototype.constructor = OutOfOrderError;
OutOfOrderError.prototype.isOutOfOrder = true;

/**
 * This client can be used to execute the 0MQ Raft RPC protocol on a single peer server.
**/
class ZmqRaftPeerClient extends ZmqProtocolSocket {
  /**
   * Create an instance of ZmqRaftPeerClient.
   * 
   * `options`:
   * 
   * - `url` {string}: A single cluster peer url to connect directly to.
   * - `urls` {Array<string>}: An array of url peers; each request will be send to each one of them
   *                           in a round-robin order.
   * - `secret` {string|Buffer}: A cluster identifying string which is sent and verified against
   *                             in each message.
   * - `timeout` {int32}: A default time interval in milliseconds after which we consider a server
   *           peer to be unresponsive; if waiting for the response exceeds this interval the request
   *           will timeout; 0 or negative disables request timeout - in this instance the response
   *           is waited for indefinitely;
   *           if undefined, the default is 500 ms.
   * - `lazy` {boolean}: Specify `true` to connect lazily on the first request.
   * - `sockopts` {Object}: Specify zeromq socket options as an object e.g.: {ZMQ_IPV4ONLY: true}.
   * - `highwatermark` {number}: A shortcut to specify `ZMQ_SNDHWM` socket option for an underlying
   *                   zeromq DEALER socket; this affects how many messages are queued per server
   *                   so if one of the peers goes down this many messages are possibly lost;
   *                   setting it prevents spamming a peer with expired messages when temporary
   *                   network partition occures (default: 2).
   * 
   * @param {string|Array<string>} [urls]
   * @param {Object} [options]
   * @return {ZmqRaftPeerClient}
  **/
  constructor(urls, options) {
    if (urls && !isArray(urls) && 'object' === typeof urls) {
      options = urls, urls = undefined;
    }

    options = Object.assign({}, options);
    if (options.timeout === undefined) {
      options.timeout = SERVER_RESPONSE_TIMEOUT;
    }

    urls || (urls = options.urls || options.url);

    super(urls, options);

    this[secretBuf$] = Buffer.from(options.secret || '');
    this[timeout$] = new Set();
  }

  /**
   * Disconnect, close socket and reject all pending requests.
   * 
   * @return {ZmqRaftPeerClient}
  **/
  close() {
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
   * Invoke RequestConfig RPC.
   * 
   * The returned promise resolves to an Object with the following properties on success:
   * - `leaderId` {string|null} - ID of the current LEADER, if it's known to the responding peer.
   * - `isLeader` {boolean} - indicates the LEADER state of the responding peer.
   * - `peers` {Array<[id, url]>} - the current cluster configuration.
   * 
   * @param {int32} [timeout] - an override for the response timeout in milliseconds;
   *                the promise is rejected with TimeoutError on timeout;
   *                if this is 0, null or undefined, default timeout is used instead.
   * @return {Promise}
  **/
  requestConfig(timeout) {
    const msg = [requestConfigTypeBuf, this[secretBuf$]]
        , options = {protocol: requestConfigProtocol, timeout};

    return this.request(msg, options)
    .then(([isLeader, leaderId, peers]) => ({leaderId, isLeader, peers}))
  }

  /**
   * Invoke RequestLogInfo RPC.
   * 
   * The returned promise resolves to an Object with the following properties:
   * 
   *  - `isLeader` {boolean}
   *  - `leaderId` {string|null}
   *  - `currentTerm` {number}
   *  - `firstIndex` {number}
   *  - `lastApplied` {number}
   *  - `commitIndex` {number}
   *  - `lastIndex` {number}
   *  - `snapshotSize` {number}
   *  - `pruneIndex` {number}
   * 
   * @param {int32} [timeout] - an override for the response timeout in milliseconds;
   *                the promise is rejected with TimeoutError on timeout;
   *                if this is 0, null or undefined, default timeout is used instead.
   * @return {Promise}
  **/
  requestLogInfo(timeout) {
    if (arguments.length === 2) { /* allow ZmqRaftClient API call */
      timeout = arguments[1];
    }
    const msg = [requestLogInfoTypeBuf, this[secretBuf$]]
        , options = {protocol: requestLogInfoProtocol, timeout};

    return this.request(msg, options)
    .then(res => {
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
    });
  }

  /**
   * Invoke ConfigUpdate RPC.
   * 
   * The returned promise resolves to the log index of the committed entry of Cold,new on success.
   * 
   * The promise is rejected with the PeerNotLeaderError if the server peer responds but its
   * current raft state is other than the LEADER.
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
   * @param {Array|Buffer} peers - the new cluster configuration.
   * @param {int32} [timeout] - an override for the response timeout in milliseconds;
   *                the promise is rejected with TimeoutError on timeout;
   *                if this is 0, null or undefined, default timeout is used instead.
   * @return {Promise}
  **/
  configUpdate(id, peers, timeout) {
    if (id === undefined) return Promise.reject(new Error("configUpdate: required id argument is missing"));
    if (!Buffer.isBuffer(peers)) {
      try {
        peers = encodeMsgPack( Array.from(parsePeers(peers)) );
      } catch(err) { return Promise.reject(err); }
    }
    const msg = [configUpdateTypeBuf, this[secretBuf$], peers]
        , options = {
            id,
            timeout,
            protocol: configUpdateProtocol,
            onresponse: (res, reply, refresh) => {
              /* response that update was accepted */
              if (res[0] === 1 && res[1] === undefined) {
                debug('accepted update');
                refresh();
              }
              else return res;
            }
          };

    return this.request(msg, options)
    .then(res => {
      var status = res[0]
        , arg = res[1];
      /* response that update was committed */
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
      /* not a leader */
      throw new PeerNotLeaderError(arg);
    });
  }

  /**
   * Invoke RequestUpdate RPC.
   * 
   * The returned promise resolves to the log index of the committed entry.
   * 
   * The promise is rejected with the PeerNotLeaderError if the server peer responds but its
   * current raft state is other than the LEADER.
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
   * @param {int32} [timeout] - an override for the response timeout in milliseconds;
   *                the promise is rejected with TimeoutError on timeout;
   *                if this is 0, null or undefined, default timeout is used instead.
   * @return {Promise}
  **/
  requestUpdate(id, data, timeout) {
    if (id === undefined) return Promise.reject(new Error("requestUpdate: required id argument is missing"));
    const msg = [requestUpdateTypeBuf, this[secretBuf$], data]
        , options = {
            id,
            timeout,
            protocol: requestUpdateProtocol,
            onresponse: (res, reply, refresh) => {
              /* response that update was accepted */
              if (res[0] === true && res[1] === undefined) {
                debug('accepted update');
                refresh();
              }
              else return res;
            }
          };

    return this.request(msg, options)
    .then(res => {
      var arg = res[1];
      /* response that update was committed */
      if (res[0] === true) {
        return arg;
      }
      /* request id get too old */
      else if (arg === undefined) {
        throw new Error("requestUpdate: expired request id");
      }
      /* not a leader */
      throw new PeerNotLeaderError(arg);
    });
  }

  /**
   * Perform RequestEntries RPC.
   * 
   * Retrieve RAFT log entries using the `receiver` callback.
   * 
   * The received entries must be consumed ASAP and the retrieving of them can not be paused.
   * However it is possible to cancel the request while processing entries.
   * 
   * Use ZmqRaftPeerClient.prototype.requestEntriesStream to work with the back-pressured streams instead.
   * 
   * The returned promise will resolve to `true` if all of the requested entries were received.
   * Otherwise the promise will resolve to `false` if the request was canceled by the receiver.
   * 
   * The promise is rejected with the PeerNotLeaderError if the server peer responds but its
   * current raft state is other than the LEADER. It may be possible even if some entries have
   * been already received.
   * 
   * If the promise is rejected with any error, that error will have a property `requestEntriesState`
   * set to a value with the following properties:
   * - lastIndex {number}
   * - [count] {number}
   * - [snapshotOffset] {number}
   * which can be used for the request continuation.
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
   * @param {int32} [timeout] - an override for the response timeout in milliseconds;
   *                            the promise is rejected with TimeoutError on timeout;
   *                            if this is 0, null or undefined, default timeout is used instead.
   * @param {number} [snapshotOffset] - a hint for the server to start responding with snapshot chunks
   *         starting from this byte offset, providing the `lastIndex` precedes the actual snapshot index.
   * @return {Promise}
  **/
  requestEntries(lastIndex, count, receiver, timeout, snapshotOffset) {
    if ('function' === typeof count) {
      timeout = receiver, receiver = count, count = undefined;
    }
    else {
      count = +count;
      if (!isFinite(count) || count <= 0) count = undefined;
      if ('function' !== typeof receiver) {
        return Promise.reject(new TypeError('receiver must be a function'));
      }
    }
    var currentByte = parseInt(snapshotOffset)||0
      , prevIndex = lastIndex;
    if (currentByte < 0) currentByte = 0;
    const msg = [requestEntriesTypeBuf, this[secretBuf$], lastIndex]
        , options = {
            timeout,
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
                /* not a leader */
                return arg;
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
                  if (currentByte !== byteOffset) {
                    throw new OutOfOrderError("requestEntries: snapshot chunks received out of order");
                  }
                  currentByte = byteOffset + chunk.length;
                  isLastChunk = currentByte === snapshotSize;
                  ret = receiver(status, chunk, lastIndex, byteOffset, snapshotSize, isLastChunk, snapshotTerm);
                  if (isLastChunk) {
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
                  else { /* not the last chunk */
                    if (count === undefined) msg[3] = null;
                    msg[4] = currentByte;
                  }
                }
                else { /* handle log entries */
                  if (msg.length === 5) {
                    msg.length = (count !== undefined) ? 4 : 3;
                  }
                  msg[2] = lastIndex;
                  res.splice(0, 3);
                  numEntries = res.length;
                  if (lastIndex - numEntries !== prevIndex) {
                    throw new OutOfOrderError("requestEntries: log entries received out of order");
                  }
                  ret = receiver(status, res, lastIndex);
                  if (count !== undefined) msg[3] = (count -= numEntries);
                }

                prevIndex = lastIndex;

                if (status === RE_STATUS_MORE) {
                  /* request more or ask to stop */
                  if (ret === false || count <= 0) {
                    /* set count = 0 to stop streaming from the server */
                    msg[3] = 0;
                  } else {
                    refresh(); /* restart timeout */
                  }
                  reply(msg);
                  if (count <= 0) return true; /* the last entry received */
                  if (ret === false) return false; /* cancel before the last entry */
                }
                else if (status === RE_STATUS_LAST) {
                  return true; /* the last entry received */
                }

                break;

              default:
                throw new Error("unknown status");
              }
            }
          };

    if (count !== undefined) {
      msg.push(count);
    }
    else if (currentByte !== 0) {
      msg.push(null);
    }
    if (currentByte !== 0) msg.push(currentByte);

    return this.request(msg, options)
    .then(result => {
      /* all received */
      if (typeof result === 'boolean') {
        return result;
      }
      /* not a leader */
      throw new PeerNotLeaderError(result);
    })
    .catch(err => {
      /* attach the state so the request can be continued from another peer */
      err.requestEntriesState = {lastIndex: msg[2], count: msg[3], snapshotOffset: msg[4]};
      throw err;
    });
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
   * The stream will emit an "error" event with the PeerNotLeaderError if the server peer responds
   * but its current raft state is other than the LEADER. It may be possible even if some entries have
   * been already received.
   * 
   * `options`:
   * 
   * - `count` {number} - the maximum number of requested entries, only valid if > 0; otherwise ignored.
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
   * @param {Object|number} [options|count]
   * @return {RequestEntriesStream}
  **/
  requestEntriesStream(lastIndex, options) {
    return new RequestEntriesStream(this, lastIndex, options);
  }

}

class RequestEntriesStream extends Readable {
  constructor(client, prevIndex, options) {
    var count, snapshotOffset, timeout;
    if ('number' === typeof options) {
      count = options;
      options = undefined;
    }
    else if (options) {
      count = options.count;
      timeout = options.timeout;
      snapshotOffset = options.snapshotOffset;
    }
    count = +count;
    if (!isFinite(count) || count <= 0) count = undefined;

    var index0 = prevIndex;

    super(Object.assign({}, options, {objectMode: true}));

    const receiver = (status, chunk, lastIndex, byteOffset, snapshotSize, isLastChunk, snapshotTerm) => {
      var more = true;
      if (status === RE_STATUS_SNAPSHOT) {
        if (isLastChunk) {
          prevIndex = lastIndex;
          snapshotOffset = undefined;
        }
        else {
          snapshotOffset = byteOffset + chunk.length;
        }
        more = this.push(
          bufferToSnapshotChunk(chunk, lastIndex, byteOffset, snapshotSize, snapshotTerm)
        );
      }
      else {
        prevIndex = lastIndex;
        const numEntries = chunk.length;
        let index = lastIndex - numEntries;
        for(let i = 0; i < numEntries; ++i) {
          more = this.push(
            bufferToLogEntry(chunk[i], ++index)
          );
        }
      }
      return more;
    };

    const request = this[request$] = () => {
      this[request$] = null;
      debug("request entries stream start %s, %s (%s)", prevIndex, count, snapshotOffset);
      return client.requestEntries(prevIndex, count, receiver, timeout, snapshotOffset)
      .then(res => {
        if (res) {
          /* EOS */
          debug("request entries stream end");
          this.push(null);
        }
        else { /* back-pressured */
          debug("request entries stop");
          if (count !== undefined) {
            count -= prevIndex - index0;
            index0 = prevIndex;
          }
          this[request$] = request;
        }
      });
    };
  }

  _read(_size) {
    var request = this[request$];
    if (request != null) {
      request().catch(err => this.emit('error', err));
    }
  }
}

ZmqRaftPeerClient.PeerNotLeaderError = PeerNotLeaderError;
ZmqRaftPeerClient.OutOfOrderError = OutOfOrderError;
ZmqRaftPeerClient.ZmqRaftPeerClient = ZmqRaftPeerClient;
ZmqRaftPeerClient.TimeoutError = TimeoutError;
ZmqRaftPeerClient.RequestEntriesStream = RequestEntriesStream;
module.exports = exports = ZmqRaftPeerClient;
