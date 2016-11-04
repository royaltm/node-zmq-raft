/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

/*

  sock = new ZmqRaftClient({peers: [{id: '01', url: 'tcp://127.0.0.1:1234'}], secret: '', timeout: 500});
  sock.requestConfig([{timeout: 500, id: Buffer.from('xxx')}])
  sock.requestEntries(prevIndex[, count][, (entries) => {  })][, {timeout: 500, id: Buffer.from('xxx')}])
  sock.requestUpdate(id, data, {timeout: 500, id: Buffer.from('xxx')}])

  // handle response
  .then(resp => console.log(resp))
  // handle timeout or close error
  .catch(err => console.error(err));

*/

const isArray = Array.isArray
    , now     = Date.now;

const { assertConstantsDefined, parsePeers, delay } = require('../utils/helpers');

/* expect response within this timeout, if not will try the next server */
const { SERVER_RESPONSE_TTL

      , RE_STATUS_NOT_LEADER
      , RE_STATUS_LAST
      , RE_STATUS_MORE
      , RE_STATUS_SNAPSHOT

      , REQUEST_CONFIG
      , REQUEST_UPDATE
      , REQUEST_ENTRIES
      , REQUEST_LOG_INFO

      , SERVER_ELECTION_GRACE_MS
      } = require('../common/constants');

assertConstantsDefined({
  SERVER_RESPONSE_TTL
, RE_STATUS_NOT_LEADER
, RE_STATUS_LAST
, RE_STATUS_MORE
, RE_STATUS_SNAPSHOT
, SERVER_ELECTION_GRACE_MS
}, 'number');

assertConstantsDefined({
  REQUEST_CONFIG
, REQUEST_UPDATE
, REQUEST_ENTRIES
, REQUEST_LOG_INFO
}, 'string', true);

const requestConfigTypeBuf  = Buffer.from(REQUEST_CONFIG);
const requestUpdateTypeBuf  = Buffer.from(REQUEST_UPDATE);
const requestEntriesTypeBuf = Buffer.from(REQUEST_ENTRIES);
const requestLogInfoTypeBuf = Buffer.from(REQUEST_LOG_INFO);

const { createFramesProtocol } = require('../protocol');

const requestConfigProtocol = createFramesProtocol('RequestConfig');
const requestUpdateProtocol = createFramesProtocol('RequestUpdate');
const requestEntriesProtocol = createFramesProtocol('RequestEntries');
const requestLogInfoProtocol = createFramesProtocol('RequestLogInfo');

const ZmqProtocolSocket = require('../client/zmq_protocol_socket');

const TimeoutError = ZmqProtocolSocket.TimeoutError;

const secretBuf$ = Symbol('secretBuf');

const debug = require('debug')('raft-client');

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
   * - `url` {String}: an url to fetch peers from
   * - `urls` {Array}: an of urls to fetch peers from
   * - `peers` {Array}: array of raft server descriptors (peers has precedence over urls)
   * - `secret` {String}: a secret to send to servers
   * - `timeout` {number}: time in milliseconds after which we consider server
   *    as dead and we'll try another one
   * - `lazy` {boolean}: specify `true` to connect lazily on first request
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
    var options = Object.assign({timeout: SERVER_RESPONSE_TTL}, options);

    super(urls, options);

    this.peers = peers;
    this.leaderId = null;
    this[secretBuf$] = Buffer.from(String(options.secret || ''));
  }

  /**
   * @property peers {Map}
  **/

  /**
   * @property leaderId {string|null}
  **/

  /**
   * Set new leader will reconnect if necessary
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
        debug('leader established: (%s) at %s', leaderId, url);
        this.leaderId = leaderId;
        this.setUrls(url);
      }
      else {
        debug('leader unknown');
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
   * @param {number} [timeout] optional timeout after which request is abandoned
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
        this.setPeers(peers, leaderId);
        var urls = {};
        peers.forEach(([id, url]) => (urls[id] = url));
        return {leaderId, urls};
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
   * Send request log info rpc
   *
   * Returned promise resolves to {
   *  isLeader,
   *  leaderId,
   *  currentTerm,
   *  firstIndex,
   *  lastApplied,
   *  commitIndex,
   *  lastIndex,
   *  snapshotSize
   * }
   *
   * By default it will repeat requests forever until resolved or errored
   *
   * @param {bool} anyPeer response from not a leader is ok
   * @param {number} [timeout] optional timeout after which request is abandoned
   * @return {Promise}
  **/
  requestLogInfo(anyPeer, timeout) {
    var expire;
    timeout |= 0;
    if (timeout !== 0) expire = now() + timeout;
    const msg = [requestLogInfoTypeBuf, this[secretBuf$]]
        , options = {protocol: requestLogInfoProtocol}
        , request = () => {
      return this.request(msg, options)
      .then(res => {
        if (!anyPeer && !res[0]) {
          /* expire now if must */
          if (expire !== undefined && now() >= expire)
            throw new TimeoutError();
          /* not a leader, re-try */
          debug('log info from not a leader, re-trying next one');
          this.setLeader(res[1]);
          return this.leaderId === null ? delay(SERVER_ELECTION_GRACE_MS).then(request)
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
        ] = res;

        if (leaderId !== null) this.setLeader(leaderId);

        return {
          isLeader,
          leaderId,
          currentTerm,
          firstIndex,
          lastApplied,
          commitIndex,
          lastIndex,
          snapshotSize
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
   * Send request update rpc
   *
   * On success it will resolve to log index of the commited and applied entry
   *
   * By default it will repeat requests (perhaps to many peers) until resolved
   * or errored (expired).
   *
   * `id` must be a 12 byte buffer or 24 byte hexadecimal string, following this:
   * https://docs.mongodb.com/manual/reference/method/ObjectId/ specification.
   * `id` should be freshly generated. Its "the seconds" part is important
   * because update requests might expire after `common.constants.REQUEST_UPDATE_TTL`
   * milliseconds.
   *
   * You may use `utils.id.getIdent()` function to generate them.
   *
   * @param {string|Buffer} id - update request id to ensure idempotent updates
   * @param {Buffer} data to append to the log
   * @param {number} [timeout] optional timeout after which request is abandoned
   * @return {Promise}
  **/
  requestUpdate(id, data, timeout) {
    // TODO: id freshness
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
        return this.leaderId === null ? delay(SERVER_ELECTION_GRACE_MS).then(request)
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
   * Send request entries rpc
   *
   * On success it will resolve to true or false if canceled by receiver
   *
   * By default it will repeat requests forever until resolved or errored
   *
   * receiver may modify entries array (e.g. clear it if has processed them)
   * stops receiving entries if the receiver returns false (exactly false, not falsyish)
   *
   * receiver signature:
   * (status, entries or chunk{Array|Buffer}, lastIndex{number} [,
   *   byteOffset{number}, snapshotSize{number}, isLastChunk{bool}]) => bool
   *
   * @param {number} lastIndex
   * @param {number} [count]
   * @param {Function} receiver
   * @param {number} [timeout] optional timeout after which request is abandoned
   * @return {Promise}
  **/
  requestEntries(lastIndex, count, receiver, timeout) {
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
                , byteOffset, snapshotSize, chunk
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
                lastIndex = msg[2] = res[2];
                /* push entries to result */
                if (status === RE_STATUS_SNAPSHOT) {
                  byteOffset = arg[0];
                  snapshotSize = arg[1];
                  chunk = res[3];
                  if (currentByte !== byteOffset) throw new OutOfOrderError("ZmqRaftClient.requestEntries: snapshot chunks not in order");
                  currentByte = byteOffset + chunk.length;
                  isLastChunk = currentByte === snapshotSize;
                  ret = receiver(status, chunk, lastIndex, byteOffset, snapshotSize, isLastChunk);
                  status = (isLastChunk ? RE_STATUS_LAST
                                        : RE_STATUS_MORE);
                }
                else {
                  res.splice(0, 3);
                  if (lastIndex - res.length !== prevIndex) throw new OutOfOrderError("ZmqRaftClient.requestEntries: entries not in order");
                  prevIndex = lastIndex;
                  ret = receiver(status, res, lastIndex);
                }

                if (status === RE_STATUS_MORE) {
                  /* request more or ask to stop */
                  if (ret === false) {
                    status = RE_STATUS_LAST;
                    /* add count = 0 to stop streaming from the server */
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
          return delay(SERVER_ELECTION_GRACE_MS).then(request);
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

}


ZmqRaftClient.OutOfOrderError = OutOfOrderError;
ZmqRaftClient.ZmqRaftClient = ZmqRaftClient;
ZmqRaftClient.TimeoutError = TimeoutError;
module.exports = exports = ZmqRaftClient;
