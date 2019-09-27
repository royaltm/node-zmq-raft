/* 
 *  Copyright (c) 2018-2019 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const assert = require('assert');
const path = require('path');
const fs = require('fs');
const {parse: urlParse, format: formatUrl} = require('url');
const {parse: parseQuery} = require('querystring');
const PROTOCOLS = {
  http: require('http'),
  https: require('https')
};
const STATUS_CODES = PROTOCOLS.http.STATUS_CODES;
const stringify = JSON.stringify;

const ZmqProtocolSocket = require('../client/zmq_protocol_socket');
const PEER_WEBMONITOR_URL_TIMEOUT = 2000;
const PEER_WEBMONITOR_URL_RETRIES = 5;
const requestWebmonitorUrlTypeBuf  = Buffer.from('W');
const REQUEST_WEBMONITOR_URL_MATCH = requestWebmonitorUrlTypeBuf[0];

const STATIC_DIR = path.resolve(__dirname, '..', '..', 'static');
const INDEX_FILE = 'index.html';
const ASSETS_DIR = 'assets';

const { FSM_CLIENT
      , FSM_FOLLOWER
      , FSM_CANDIDATE
      , FSM_LEADER
      } = require('../common/constants');

const debug = require('debug')('zmq-raft:webmonitor');

const ServerSideEvents = require('../common/sse');

/*

A simple ZMQ RAFT monitoring http server. Responds to the following urls:

- GET `/` or `/index.html`: sends a webmonitor client html page.
- GET `/assets/*` sends a file from `assets_dir` directory.
- GET `/status`: a json containing current `peers`.
- GET `/peer?id=...`: a json with the webmonitor url of the provided peer's `id`.
- GET `/monitor`: a Server Side Events stream which sends heartbeats as RAFT
  status messages or RAFT config changes.

@property {string} url - an announced webmonitor base url
@property {number} port - an announced webmonitor port
@property {string} proto - a webmonitor server protocol being used "http" or "https"
@property {ZmqRaft} zmqRaft - a monitored instance of ZMQ RAFT
@property {Set<ServerSideEvents>} monitors - a set of connected clients
@property {string} index_file - a path to `index.html` file
@property {string} assets_dir - a path to `assets` directory

*/
class WebMonitor {
  /**
   * Creates a new WebMonitor instance.
   *
   * WebMonitor instance will bind to the provided tcp host and port and will close
   * itself once the `zmqRaft` instance is closed.
   *
   * `options`:
   *
   * - `proto` {string|null}: An announced (on an external interface) protocol.
   *           Specifying "https" will only change the announced url. To create a `https` server,
   *           use `bind.proto` option. Not specifying `proto` or providing `null` will defer to the
   *           `bind.proto` or to the default: "http".
   * - `host` {string}: An announced (on an external interface) host name (default: "localhost").
   * - `port` {number}: An anonunced (on an external interface) port (default: 8050).
   * - `bind` {Object}: An optional server bind options: `host`, `port` and `proto`; Specify "https"
   *          as `bind.proto` for a HTTPS server. Any other option is being passed as `options`
   *          to [bind.proto].createServer(). A "*" or "::" can be passed to `bind.host` to bind to all
   *          local interfaces.
   * - `static_dir` {string}: A directory where static files are placed.
   * - `index_file` {string}: A path to `index.html` file relative to `static_dir`.
   * - `assets_dir` {string}: A path to `assets` directory relative to `static_dir`.
   *
   * @param {ZmqRaft} zmqRaft - a monitored instance of ZMQ RAFT
   * @param {Object} options
   * @return {WebMonitor}
  **/
  constructor(zmqRaft, { port=8050
                       , host='localhost'
                       , proto
                       , bind
                       , static_dir=STATIC_DIR
                       , index_file=INDEX_FILE
                       , assets_dir=ASSETS_DIR }) {

    this.port = port;
    this.monitors = new Set();
    this.index_file = index_file = path.resolve(static_dir, index_file);
    this.assets_dir = assets_dir = path.resolve(static_dir, assets_dir);
    this.peerWebUrls = new Map();

    debug('index file: %s', index_file);
    debug('assets directory: %s', assets_dir);

    bind = Object.assign({}, bind);

    this.url = formatUrl({protocol: proto || bind.proto || "http", hostname: host, port});

    debug('url: %s', this.url);

    if ('string' === typeof bind.host) {
      host = bind.host;
    }
    else if ('number' === typeof bind.port) {
      port = bind.port;
    }

    this.proto = proto = bind.proto || "http";
    delete bind.host;
    delete bind.port;
    delete bind.proto;

    proto = PROTOCOLS[proto];
    if (proto == null) throw new Error("unknown webmonitor protocol");

    const webmonitorUrlBuf = Buffer.from(this.url);
    /* eavesdrop on the state machine requests and handle our own */
    zmqRaft._stateMachine.on('client-request', (reply, msgType) => {
      if (msgType.length === 1 && msgType[0] === REQUEST_WEBMONITOR_URL_MATCH) {
        debug('replying with url: %s', webmonitorUrlBuf);
        reply(webmonitorUrlBuf);
      }
    });

    this.zmqRaft = zmqRaft
                   .on('state', () => this._zmqStateChange())
                   .on('config', () => this._zmqConfigChange())
                   .on('close', () => this.close());

    const server = this.server = proto.createServer(bind, (req, res) => this._handle(req, res));

    const listen = () => {
      debug('binding to: %s://[%s]:%s', this.proto, host, port);
      server.listen(port, host, () => {
        debug('listening at: [%s]:%s', host, port);
        this._setupInterval();
      });
    };

    server.on('error', (e) => {
      if (e.code === 'EADDRINUSE') {
        debug('Address in use, retrying...');
        setTimeout(() => {
          server.close();
          listen();
        }, 1000).unref();
      }
    });

    listen();
  }

  /**
   * Closes WebMonitor server and destroys all monitoring client connections.
  **/
  close() {
    debug('closing');
    clearInterval(this.intervalHandler);
    this.server && this.server.close();
    for(let sse of this.monitors) {
      sse.end();
    }
  }

  _configData() {
    return {peers: this.zmqRaft.peersAry, label: this.zmqRaft.label};
  }

  _configPayload() {
    return {event: "config", data: this._configData()};
  }

  _setupInterval() {
    clearInterval(this.intervalHandler);
    this.intervalHandler = setInterval(() => this._heartbeat(), 1000);
  }

  _handle(req, res) {
    const method = req.method;
    if (method !== 'GET' && method !== 'HEAD') {
      res.setHeader('Allow', 'GET, HEAD');
      return httpError(req, res, 405);
    }

    const url = urlParse(req.url);

    switch(url.pathname) {
      case '/status':
        return this._sendStatus(req, res);
      case '/peer':
        return this._peerMonitorUrl(req, res, parseQuery(url.query).id);
      case '/monitor':
        return this._monitor(req, res);
      case '/':
      case '/index.html':
        return sendFile(this.index_file, req, res);
      default:
        if (url.pathname.startsWith('/assets/')) {
          return sendFile(path.join(this.assets_dir, url.pathname.substr(5)), req, res);
        }
        else return httpError(req, res, 404);
    }
  }

  _sendStatus(req, res) {
    sendJson(this._configData(), req, res);
  }

  _peerMonitorUrl(req, res, peerId) {
    debug('url request for peer: %s', peerId);
    if (peerId === this.zmqRaft.peerId) {
      sendJson({id:peerId, url:this.url}, req, res);
    }
    else if (!peerId) {
      httpError(req, res, 400);
    }
    else if (!this.zmqRaft.peers.has(peerId)) {
      httpError(req, res, 404);
    }
    else {
      this._fetchPeerWebUrl(peerId)
      .then(peerUrl => sendJson({id:peerId, url:peerUrl}, req, res),
            err => httpError(req, res, 503));
    }
  }

  _fetchPeerWebUrl(peerId) {
    var res = this.peerWebUrls.get(peerId);
    if (res === undefined) {
      let failures = 0;
      const peerUrl = this.zmqRaft.peers.get(peerId);
      const sock = new ZmqProtocolSocket(peerUrl, {timeout: PEER_WEBMONITOR_URL_TIMEOUT});
      const request = () => sock.request([requestWebmonitorUrlTypeBuf, this.zmqRaft._secretBuf])
      .then(([peerUrlBuf]) => {
        sock.destroy();
        return peerUrlBuf.toString();
      })
      .catch(err => {
        if (err.isTimeout && (failures++ < PEER_WEBMONITOR_URL_RETRIES)) {
          return request();
        }
        else {
          this.peerWebUrls.delete(peerId);
          sock.destroy();
          throw err;
        }
      });
      res = request();
      this.peerWebUrls.set(peerId, res);
    }
    return res;
  }

  _zmqStateChange() {
    this._setupInterval();
    setImmediate(() => this._heartbeat());
  }

  _zmqConfigChange() {
    this.zmqRaft.cluster.updateOtherPeersMap(this.peerWebUrls);
    setImmediate(() => this._broadcast('_configPayload'));
  }

  _monitor(req, res) {
    const id = req.headers['last-event-id'];
    if (id !== undefined && id !== this.zmqRaft.peerId) {
      return httpError(req, res, 400);
    }

    res.setHeader('Access-Control-Allow-Origin', '*');
    const sse = ServerSideEvents.create(req, res, {retry: 2000});
    sse.on('close', () => this.monitors.delete(sse));
    this.monitors.add(sse);
    sse.send(this._monitorPayload());
  }

  _monitorPayload() {
    const zmqRaft = this.zmqRaft
        , log = zmqRaft._log
        , snapshot = log.snapshot;

    var state = zmqRaft.state;

    switch(state) {
      case FSM_CLIENT:    state = 'Client'; break;
      case FSM_FOLLOWER:  state = 'Follower'; break;
      case FSM_CANDIDATE: state = 'Candidate'; break;
      case FSM_LEADER:    state = 'Leader'; break;
    }

    return {
      id: zmqRaft.peerId,
      data: {
        state: state,
        currentTerm: zmqRaft.currentTerm,
        votedFor: zmqRaft.votedFor,
        lastApplied: zmqRaft.lastApplied,
        commitIndex: zmqRaft.commitIndex,
        firstIndex: log.firstIndex,
        lastIndex: log.lastIndex,
        snapshot: {
          logTerm: snapshot.logTerm,
          logIndex: snapshot.logIndex,
          dataSize: snapshot.dataSize
        }
      }
    };
  }

  _heartbeat() {
    this._broadcast('_monitorPayload');
  }

  _broadcast(payloadMethod) {
    const monitors = this.monitors;
    if (monitors.size !== 0) {
      const payload = this[payloadMethod]();
      for(let sse of monitors) {
        sse.send(payload);
      }
    }
  }
}

function sendFile(filepath, req, res) {
  debug('sending %s', filepath);
  fs.createReadStream(filepath, {encoding: 'utf8'})
  .on('error', err => httpError(req, res, 404))
  .pipe(res);
}

function sendJson(json, req, res) {
  if ('string' !== typeof json) json = stringify(json);
  res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': json.length });
  res.end(json);
}

function httpError(req, res, status=500, body=STATUS_CODES[status]) {
  res.writeHead(status, { 'Content-Type': 'text/plain', 'Content-Length': body.length });
  res.end(body);
}

module.exports = exports = WebMonitor;
