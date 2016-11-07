/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";
const zmq = require('zmq');
const debug = require('debug')('zmq-raft:socket');

const isArray = Array.isArray;
const $connected      = Symbol.for('connected');
const $sockopts       = Symbol.for('sockopts');
const $connectUrls    = Symbol.for('connectUrls');
const $disconnectUrls = Symbol.for('disconnectUrls');

class ZmqBaseSocket {

  constructor(urls, options) {
    if (urls && 'object' === typeof urls && !isArray(urls)) {
        options = urls, urls = options.urls || options.url;
    }
    if ('number' === typeof options) {
      options = {timeout: options};
    }
    options || (options = {});
    var sockopts = options.sockopts || {};
    if ('object' !== typeof sockopts)
      throw TypeError(`${this.constructor.name}: sockopts must be an object`);
    this.urls = [];
    this.addUrls(urls);
    this.options = Object.assign({}, options);
    this[$sockopts] = Object.keys(sockopts).filter(opt => sockopts.hasOwnProperty(opt))
      .reduce((map, opt) => { map.set(toZmqOpt(opt), sockopts[opt]); return map; }, new Map());
    this.socket = null;
    this[$connected] = false;
  }

  /**
   * @property urls {Array} - list of url strings that socket is connected to
  **/

  /**
   * @property connected {boolean}
  **/
  get connected() {
    return this[$connected];
  }

  /**
   * Add new urls to server pool and optionally connect to them
   *
   * @param {string|Array} ...urls
   * @return {ZmqDealerSocket}
  **/
  addUrls(...urls) {
    // flatten arguments
    urls = [].concat(...urls);
    if (!urls.every(url => ('string' === typeof url && url.length !== 0))) {
      throw TypeError(`${this.constructor.name}: please specify urls as zmq connection strings`);
    }
    var currentUrls = new Set(this.urls);
    var newUrls = [];
    for(var url of urls) {
      if (!currentUrls.has(url)) {
        currentUrls.add(url);
        newUrls.push(url);
      }
    }
    this[$connectUrls](newUrls);
    this.urls = Array.from(currentUrls);
    return this;
  }

  /**
   * Remove urls from current server pool and disconnect from them
   *
   * @param {string|Array} ...urls
   * @return {ZmqDealerSocket}
  **/
  removeUrls(...urls) {
    // flatten arguments
    urls = [].concat(...urls);
    if (!urls.every(url => ('string' === typeof url && url.length !== 0))) {
      throw TypeError(`${this.constructor.name}: please specify urls as zmq connection strings`);
    }
    var currentUrls = new Set(this.urls);
    var removeUrls = [];
    for(var url of urls) {
      if (currentUrls.has(url)) {
        currentUrls.delete(url);
        removeUrls.push(url);
      }
    }
    this[$disconnectUrls](removeUrls);
    this.urls = Array.from(currentUrls);
    return this;
  }

  /**
   * Connect to the new set of urls
   *
   * Unchanged urls will keep the current server connection.
   *
   * @param {string|Array} ...urls
   * @return {ZmqDealerSocket}
  **/
  setUrls(...urls) {
    // flatten arguments
    urls = [].concat(...urls);
    if (!urls.every(url => ('string' === typeof url && url.length !== 0))) {
      throw TypeError(`${this.constructor.name}: please specify urls as zmq connection strings`);
    }
    var currentUrls = new Set(this.urls);
    var newUrls = [];
    for(var url of urls) {
      if (!currentUrls.has(url)) {
        currentUrls.add(url);
        newUrls.push(url);
      }
    }
    var removeUrls = [];
    var urlsToSet = new Set(urls);
    for(var url of this.urls) {
      if (!urlsToSet.has(url)) {
        currentUrls.delete(url);
        removeUrls.push(url);
      }
    }
    this[$disconnectUrls](removeUrls);
    this[$connectUrls](newUrls);
    this.urls = Array.from(currentUrls);    
    return this;
  }

  /**
   * Disconnect, close socket, reject all pending requests and prevent further ones
  **/
  destroy() {
    this.close();
    this.urls = undefined;
    this.request = destroyed();
    this.connect = destroyed();
  }

  /**
   * Disconnect, close socket and reject all pending requests
   *
   * @return {ZmqDealerSocket}
  **/
  close() {
    if (!this[$connected]) return this;
    this[$disconnectUrls](this.urls)
    this.socket.close();
    this.socket = null;
    this[$connected] = false;
    return this;
  }

  /**
   * Get zmq socket option from the underlaying socket
   *
   * @param {string} opt
   * @return {*}
  **/
  getsockopt(opt) {
    return this[$sockopts].get(toZmqOpt(opt));
  }

  /**
   * Set zmq socket option on the underlaying socket
   *
   * @param {string} opt
   * @param {*} value
   * @return {ZmqDealerSocket}
  **/
  setsockopt(opt, value) {
    opt = toZmqOpt(opt);
    this[$sockopts].set(opt, value);
    if (this.socket) this.socket.setsockopt(opt, value);
    return this;
  }

  [$connectUrls](urls) {
    var socket = this.socket;
    if (socket) {
      for(var url of urls) {
        debug("socket.connect: %s", url);
        socket.connect(url);
      }
    }
  }

  [$disconnectUrls](urls) {
    var socket = this.socket;
    if (socket) {
      for(var url of urls) {
        debug("socket.disconnect: %s", url);
        socket.disconnect(url);
      }
    }
  }
}

function destroyed() {
  throw new Error(`${this.constructor.name}: socket destroyed`);
}

function toZmqOpt(opt) {
  var value = ('string' === typeof opt) ? zmq[opt] : opt;
  if ('number' !== typeof value || !isFinite(value)) {
    throw TypeError(`ZmqBaseSocket: invalid socket option: ${opt}`);
  }
  return value;
}

module.exports = ZmqBaseSocket;
