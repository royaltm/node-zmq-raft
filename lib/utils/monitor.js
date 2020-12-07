const EventEmitter = require('events');
const debug = require('debug')('zmq-raft-monitor');
const { createRangeRandomizer } = require('./helpers');
const ZmqRaftClient = require('../client/zmq_raft_client');
const ZmqRaftPeerClient = require('../client/zmq_raft_peer_client');
const TimeoutError = ZmqRaftClient.TimeoutError;
const { parsePeers } = require('../utils/helpers');

const REQUEST_CONFIG_TIMEOUT = 10000; // 10 seconds
const REQUEST_INFO_TIMEOUT = 4000; // 4 seconds
const REQUEST_INFO_INTERVAL = 2500; // 2.5 seconds
const QUERY_DELAY_FIRST = 100;
const QUERY_DELAY_STEP = 300;

class ZmqRaftMonitor extends EventEmitter {
  /**
   * Create ZmqRaftMonitor
   *
   * `options` may be one of:
   *
   * - `url` {String}: A seed url to fetch peer urls from via a Request Config RPC.
   * - `urls` {Array}: An array of seed urls to fetch peers from via a Request Config RPC.
   * - `peers` {Array<[id,url]>|{id: url}}: An array or an Object map of established zmq raft server
   *                 descriptors; `peers` has precedence over `urls` and if provided the peer list
   *                 is not being fetched via Request Config RPC.
   * - `secret` {String|Buffer}: A cluster identifying string which is sent and verified against
   *                             in each message.
   * - `urlsOnly` {boolean}: if `true` the monitor will only connect to the peer urls provided in
   *               the `url` or `urls` option, otherwise all of the cluster peers will be monitored.
   * - `sockopts` {Object}: Specify zeromq socket options as an object e.g.: `{ZMQ_IPV4ONLY: true}`.
   * - `serverElectionGraceDelay` {number}: A delay in milliseconds to wait for the Raft peers
   *                                     to elect a new leader before retrying (default: 300 ms).
   * - `requestConfigTimeout` {number}: A timeout in ms used by the initial query to get the cluster
   *                   configuration (default: 10 seconds).
   * - `requestInfoTimeout` {number}: A timeout in ms after which the peer is considered unresponsive
   *                   (default: 4 seconds).
   * - `requestInfoInterval` {number}: A minimal value of how often (in ms) each of the raft peers is
   *                   being queried for the log status information (default: 2.5 seconds).
   * - `queryDelayFirst` {number}: A delay in ms between after receiving the cluster configuration and
   *                   before sending a query to the first peer in the cluster (default: 100 ms).
   * - `queryDelayStep` {number}: An additional delay in ms before querying each of the peers
   *                   (default: 300 ms).
   *
   * @param {Object} [options]
   * @return {ZmqRaftMonitor}
  **/
  constructor(options={}) {
    super();
    this.clients = new Map();

    options = Object.assign(
    { // defaults
      requestConfigTimeout: REQUEST_CONFIG_TIMEOUT,
      requestInfoTimeout: REQUEST_INFO_TIMEOUT,
      requestInfoInterval: REQUEST_INFO_INTERVAL,
      queryDelayFirst: QUERY_DELAY_FIRST,
      queryDelayStep: QUERY_DELAY_STEP
    },
    options,
    { // overrides
      heartbeat: 0,
      lazy: false,
      highwatermark: 1
    });

    if (options.timeout != null) debug('socket timeout: %s ms', options.timeout);
    if (options.serverElectionGraceDelay != null) debug('election grace: %s ms', options.serverElectionGraceDelay);
    debug('config timeout: %s ms', options.requestConfigTimeout);
    debug('peer info timeout: %s ms', options.requestInfoTimeout);
    debug('peer info interval: %s ms', options.requestInfoInterval);
    debug('query delay first: %s ms', options.queryDelayFirst);
    debug('query delay step: %s ms', options.queryDelayStep);

    var peersPromise;
    this.bootstrapClient = null;

    if (options.peers) {
      const peers = parsePeers(options.peers);
      const urls = {};
      for (let [id, url] of peers) urls[id] = url;
      peersPromise = Promise.resolve({leaderId: null, urls});
    }
    else {
      this.bootstrapClient = new ZmqRaftClient( options );
      peersPromise = this.bootstrapClient.requestConfig(options.requestConfigTimeout)
      .then(result => {
        this.bootstrapClient.destroy();
        this.bootstrapClient = null;
        return result
      });
    }

    peersPromise.then(({leaderId, urls}) => {
      const intervalRandomizer = createRangeRandomizer(options.requestInfoInterval,
                                                       options.requestInfoInterval * 2);

      var graceDelay = options.queryDelayFirst;

      var seedUrlSet;
      if (options.urlsOnly) {
        debug('monitoring only selected peers');
        seedUrlSet = new Set([].concat(options.urls || options.url));
      }

      for (const peerId in urls) {
        const peerOpts = Object.assign({}, options);
        const peerClient = new ZmqRaftPeerClient( urls[peerId], peerOpts );
        if (!seedUrlSet || seedUrlSet.has(urls[peerId])) {
          const query = (ms) => peerClient.delay( ms )
          .then(() => {
            if (peerClient.socket) {
              return peerClient.requestLogInfo(options.requestInfoTimeout)
              .then(info => {
                this.emit('info', peerId, info);
                return query(intervalRandomizer())
              },
              err => {
                this.emit('peer-error', peerId, err);
                return query(intervalRandomizer())
              });
            }
          });
          this.clients.set( peerId, peerClient );
          query(graceDelay);
          graceDelay += options.queryDelayStep;
        }
      }

      this.emit('peers', urls, leaderId);
    })
    .catch(err => {
      debug('error retrieving cluster config, terminating: %s', err);
      if (this.bootstrapClient) {
        this.bootstrapClient.destroy();
        this.bootstrapClient = null;
      }
      this.emit('error', err);
    });
  }

  close() {
    this.destroy();
    this.emit('close');
  }

  destroy() {
    if (this.bootstrapClient) {
      this.bootstrapClient.destroy();
      this.bootstrapClient = null;
    }
    for (let client of this.clients.values()) {
      client.destroy();
    }
    this.clients.clear();
  }
}

exports.ZmqRaftMonitor = ZmqRaftMonitor;
