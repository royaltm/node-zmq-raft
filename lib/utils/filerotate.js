/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const RETAIN_FILES_MAX = 60;
const ROTATE_HISTORY_DELAY_MS = 5000;

const path = require('path');
const { readdir, unlink } = require("../utils/fsutil");
const historyTimeout$ = Symbol("historyTimeout");

const emptyFunction = () => {};

var lastRotateTstr, rotateFileCounter = 0;

exports.createRotateName = function(filename) {
  var tstr = new Date().toISOString();
  rotateFileCounter = (lastRotateTstr === tstr) ? rotateFileCounter + 1 : 0;
  lastRotateTstr = tstr;
  return filename + '-' + tstr.substr(0, 10) + '-' +
        tstr.substr(11,2) + tstr.substr(14,2) + tstr.substr(17,2) + '-' +
        tstr.substr(20,3) + rotateFileCounter.toString(36);
}

/* installs triggerHistoryRotation method
   requires that this inscance has `filename` property */
exports.mixin = function(proto, debug, retainFilesMax, rotateHistoryDelayMs) {
  debug || (debug = emptyFunction);
  if (retainFilesMax === undefined) retainFilesMax = RETAIN_FILES_MAX;
  retainFilesMax = +retainFilesMax;
  if (rotateHistoryDelayMs === undefined) rotateHistoryDelayMs = ROTATE_HISTORY_DELAY_MS;
  rotateHistoryDelayMs = +rotateHistoryDelayMs;

  proto.triggerHistoryRotation = function() {
    clearTimeout(this[historyTimeout$]);
    var timeout = this[historyTimeout$] = setTimeout(() => {
      removeHistoryFiles(this.filename)
      .catch(err => debug('history rotation ERROR: %s', err));
    }, rotateHistoryDelayMs);
    /* let it go */
    timeout.unref();
    return this;
  }

  const historyFilesMatch = /^\d{4}-\d{2}-\d{2}-\d{6}-\d{3}[0-9a-z]+$/;

  function removeHistoryFiles(filename) {
    debug('retaining %s history files: "%s"', retainFilesMax, filename);
    var basename = path.basename(filename) + '-';
    var dirname = path.dirname(filename);
    var cmplength = (basename + '0000-00-00-000000-000').length;
    return readdir(dirname)
    .then(files => files
      .filter(fname => fname.startsWith(basename) && historyFilesMatch.test(fname.slice(basename.length)))
      .sort((a,b) => {
        var a1 = a.substr(0, cmplength), b1 = b.substr(0, cmplength);
        if (a1 === b1) {
          return parseInt(a.slice(cmplength), 36) - parseInt(b.slice(cmplength), 36);
        }
        return a1 < b1 ? -1 : 1;
      })
      .slice(0, -retainFilesMax)
    )
    .then(files => {
      var next = () => {
        var file = files.shift();
        if (file) {
          debug('removing history file: %s', file);
          return unlink(path.join(dirname, file)).then(next);
        }
      };
      return next();
    });
  }
};
