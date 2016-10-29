/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";
/*

This module contains modified zmq.Sockets.

Motivation:

The fire and forget zmq.Socket's OutBatch tactics does not fit our requirements.
On one hand we need to ensure deliverence of rpc packets (by repeating requests after timeout)
but when the peer on the other end of the socket dies we don't want to flood her with
queued requests when she comes back.

Both ZmqRpcSocket and ZmqProtocolSocket have different requirements but they have common ground in simplyfied
approach to sending, introduced in ZmqDealerSocket.

The other one is unnecessary argument spreading in 'message' event from zmq.Socket.

Also fixes a bug with pause.

*/
const assert = require('assert');

const isArray = Array.isArray
    , isBuffer = Buffer.isBuffer;

const { Socket, ZMQ_SNDMORE } = require('zmq');

const toBuffer = (buf) => (isBuffer(buf) ? buf
                                         : Buffer.from('string' === typeof buf ? buf : String(buf)));

/*
  This is a specialized node zmq.Socket with custom behaviour:

  Sending:

  Unchanged

  Receiving:

  Instead of 'message' event the 'frames' event is emited with only one
  argument of type Array as received from zmq binding.

  Also fixes immediate pause issue. Original zmq.Socket.prototype.pause() may not be immediate, see:
  https://github.com/JustinTulloss/zeromq.node/issues/480#issuecomment-257036713

*/
class ZmqSocket extends Socket {
  /**
   * Creates a new socket.
   *
   * @constructor
   * @api public
  **/
  constructor(type) {
    super(type);
  }

  /* pause wil stop reading immediately */
  _flushRead() {
    var frames = this._zmq.readv(); /* can throw */
    if (!frames) {
      return false;
    }

    this.emit('frames', frames);
    /* user may pause socket while handling frames so check again here */
    return !this._paused;
  }

}

exports.ZmqSocket = ZmqSocket;

/*
  This is a specialized node zmq.Socket('dealer') with custom behaviour:

  Sending:

  There are no out batches like in original zmq.Socket.

  The `send(msg) -> bool` method either pushes the message immediately to the zmq socket queue (returning true)
  or enables 'drain' events on self and returns false.
  User may try to send again on 'drain' event. Successfull send will disable 'drain' events.
  However user may also give up and disable 'drain' events calling cancelSend().

  Receiving: see ZmqSocket

*/
class ZmqDealerSocket extends ZmqSocket {
  /**
   * Creates a new ZMQ_DEALER socket.
   *
   * @constructor
   * @api public
  **/
  constructor() {
    super('dealer');

    this._zmq.onSendReady = () => {
      try {
        this.emit('drain');
      } catch(err) {
        this.emit('error', err); /* may throw */
      }
    }
  }

  /**
   * Cancel pending send
   *
   * This disables drain events.
   *
   * @return {ZmqSocket} for chaining
   * @api public
  **/
  cancelSend() {
    this._zmq.pending = false;
    return this;
  }

  /**
   * Send the given `msg`.
   *
   * Return true if msg was queued false if it wasn't
   * In case when send returns false this socket will receive 'drain' event when
   * it's ready for more messages.
   *
   * @param {String|Buffer|Array} msg
   * @return {bool} true if send was successfull or false if zmq queue is full
   * @api public
  **/
  send(msg) {
    const zmq = this._zmq;
    /* disable drain events */
    zmq.pending = false;

    var content;
    if (isArray(msg)) {
      content = [];
      let i = 0
        , last = msg.length - 1;
      if (last < 0) throw new Error("zmq.send: no message to send");
      while (i < last) {
        content.push(toBuffer(msg[i++]), ZMQ_SNDMORE);
      }
      content.push(toBuffer(msg[i]), 0);
    }
    else {
      msg = toBuffer(msg);
      content = [msg, 0];
    }

    try {
      if (zmq.sendv(content)) { /* can throw */
        /* send successfully, no drain event necessary */
        return true;
      }

      /* enable drain event */
      zmq.pending = true;

    } catch(err) {
      this.emit('error', err); /* may throw */
    }

    return false;
  }

  /* pause wil NOT stop sending (intentional) */
  _flushWrite() {
    /* no-op */
  }

  _flushWrites() {
    /* no-op */
  }

}

exports.ZmqDealerSocket = ZmqDealerSocket;
