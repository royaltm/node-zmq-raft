/* 
 *  Copyright (c) 2017 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const os = require('os');
const dns = require('dns');

const DEFAULT_REMOTE_PORT = 8047;

// const isWindows = process.platform === 'win32';

function getOptions() {
  var options = {all: true};
  /*
  if (isWindows) { /* zmq apparently can't handle IPV6 in windows */
    options.family = 4;
  }
  */
  return options;
}

const flatenUnique = (array) => Array.from(new Set([].concat(array)));

module.exports = exports = function(hosts, defaultPort) {
  hosts = flatenUnique(hosts);

  return Promise.all(
    hosts.map(host => lookup(host, defaultPort))
  ).then(results => {
    var allUrls = results.reduce((allUrls, urls) => allUrls.concat(urls), []);
    return flatenUnique(allUrls);
  });
};

const hostPortRegexp = /^([a-zA-Z0-9.-]+|\*|\[([a-zA-Z0-9%.:]+)\])(?::(\d+))?$/;

function splitHostPort(host) {
  var port, match = host.match(hostPortRegexp);

  if (match) {
    host = match[2] || match[1], port = match[3];
  }
  return {hostname: host, port};
}

function lookup(host, defaultPort) {
  const options = getOptions();

  var {hostname, port} = splitHostPort(host);
  port &= 0xffff;
  port || (port = defaultPort || DEFAULT_REMOTE_PORT);

  switch(hostname) {
    case '*':
      options.family = 4;
      /* fall through */
    case '::':
    case '[::]':
      hostname = os.hostname();
  }

  return new Promise((resolve, reject) => {
    dns.lookup(hostname, options, (err, addresses) => {
      if (err) return reject(err);
      resolve(addresses.map(({address, family}) => {
        address = (family == 4 ? address : `[${address}]`);
        return `tcp://${address}:${port}`;
      }));
    });
  });
};
