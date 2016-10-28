/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const path = require('path');
const { readdir, unlink } = require("../utils/fsutil");

const emptyFunction = () => {};

var tmpFileCounter = 0;

exports.createTempName = function(filename) {
  tmpFileCounter = ++tmpFileCounter >>> 0;
  return filename + '.tmp-' + process.pid + '-' + tmpFileCounter.toString(36);
};

const tmpFileMatch = /^\d+-[0-9a-z]+$/;

exports.cleanupTempFiles = function(filename, debug) {
  debug || (debug = emptyFunction);
  const basename = path.basename(filename) + '.tmp-'
      , baselen = basename.length
      , dirname = path.dirname(filename);

  return readdir(dirname)
  .then(files => files.filter(fname => fname.startsWith(basename)
                                    && tmpFileMatch.test(fname.slice(baselen))))
  .then(files => {
    const next = () => {
      var file = files.shift();
      if (file) {
        debug('removing temporary file: %s', file);
        return unlink(path.join(dirname, file)).then(next);
      }
    };
    return next();
  });
};
