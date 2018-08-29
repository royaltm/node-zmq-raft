/* 
 *  Copyright (c) 2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const EventEmitter = require('events');

const PassThrough = require('stream').PassThrough;

const debug = require('debug')('lib:sse');

const stringify = JSON.stringify;

var counter = 0, globalId = 0;

const nextId = () => (globalId = (globalId + 1) >>> 0).toString(16);

class ServerSideEvents extends EventEmitter {
  constructor(req, res, options) {
    super();

    this.id = nextId();
    this.req = req;
    this.res = res;

    const endproxy = this._endproxy = () => this.end();

    if (options && options.retry !== undefined) {
      const retry = options.retry
      this.send = (options) => {
        delete this.send;
        const msg = `retry: ${retry.toString()}\n${sse(options)}`;
        this.stream.write(msg);
      }
    }

    req
    .on('close',  endproxy)
    .on('error',  endproxy);

    req.setTimeout(0);

    res.writeHead(200, {'Content-Type': 'text/event-stream'});

    this.stream = res;

    const {remoteAddress, remotePort} = res.socket;

    debug('connected[%s] (%d total) %s:%d', this.id, ++counter, remoteAddress, remotePort);
  }

  send(options) {
    this.stream.write(sse(options));
  }

  comment(text) {
    text = (text || '').replace(/\r?\n/g, "\n: ");
    this.stream.write(': ' + text + '\n');
  }

  end() {
    const {req, res} = this;
    if (req == null) return;
    this.req = null;
    this.res = null;

    req
    .removeListener('close',  this._endproxy)
    .removeListener('error',  this._endproxy);

    try {
      this.emit('close');
    } catch(err) {
      this.emit('error', err);
    } finally {
      res.end();
      debug('disconnected[%s] (%d total)', this.id, --counter);
    }
  };

}

function sse({data, event, id}) {
  if ('string' !== typeof data) {
    data = stringify(data);
  }
  else {
    data = data.replace(/\r?\n/g, "\ndata: ");
  }

  var message = `data: ${ data }\n\n`;
  if ('string' === typeof event) message = `event: ${event}\n${message}`;
  if ('string' === typeof id) message = `id: ${id}\n${message}`;

  return message;
}


ServerSideEvents.prototype.sse = sse;
ServerSideEvents.sse = sse;
ServerSideEvents.create = (req, res, options) => new ServerSideEvents(req, res, options);

module.exports = exports = ServerSideEvents;
