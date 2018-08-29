/* 
 *  Copyright (c) 2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const assert = require('assert');
const path = require('path');
const fs = require('fs');
const urlParse = require('url').parse;
const http = require('http');
const STATUS_CODES = http.STATUS_CODES;
const stringify = JSON.stringify;

const STATIC_DIR = path.resolve(__dirname, '..', '..', 'static');
const INDEX_FILE = path.join(STATIC_DIR, 'index.html');
const CSS_PATH = path.resolve(STATIC_DIR, 'css');

const { FSM_CLIENT
      , FSM_FOLLOWER
      , FSM_CANDIDATE
      , FSM_LEADER
      } = require('../common/constants');

const debug = require('debug')('zmq-raft:webmonitor');

const ServerSideEvents = require('../common/sse');

class WebMonitor {
  constructor(zmqRaft, {port=8050, host='::'}) {
    this.port = port;
    this.monitors = new Set();
    this.zmqRaft = zmqRaft
                   .on('state', () => this._zmqStateChange())
                   .on('close', () => this.close());

    const server = this.server = http.createServer((req, res) => this._handle(req, res));

    const listen = () => {
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

  close() {
    debug('closing');
    clearInterval(this.intervalHandler);
    this.server.close();
    for(let sse of this.monitors) {
      sse.end();
    }
  }

  _setupInterval() {
    clearInterval(this.intervalHandler);
    this.intervalHandler = setInterval(() => this._heartbeat(), 1000);
  }

  _handle(req, res) {
      const url = urlParse(req.url);
      switch(url.pathname) {
        case '/status':
          return this._status(req, res);
        case '/monitor':
          return this._monitor(req, res);
        case '/':
          return sendFile(INDEX_FILE, req, res);
        default:
          if (url.pathname.startsWith('/css/')) {
            return sendFile(path.join(CSS_PATH, url.pathname.substr(5)), req, res);
          }
          else return httpError(req, res, 404);
      }
  }

  _zmqStateChange() {
    this._setupInterval();
    setImmediate(() => this._heartbeat());
  }

  _status(req, res) {
    sendJson({peers: this.zmqRaft.peersAry, port: this.port}, req, res);
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
    const monitors = this.monitors;
    if (monitors.size !== 0) {
      const payload = this._monitorPayload();
      for(let sse of monitors) {
        sse.send(payload);
      }
    }
  }
}

function sendFile(filepath, req, res) {
  debug('sending %s', filepath);
  fs.createReadStream(filepath, {encoding: 'utf8'})
  .on('error', err => httpError(req, res, 404, err.message))
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
