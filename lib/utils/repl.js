/*
 *  Copyright (c) 2016-2018 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const os = require('os');
const fs = require('fs');
const path = require('path');
const REPL = require('repl');
const readline = require('readline');

exports.REPL = REPL;
exports.readline = readline;

exports.createRepl = function(opts) {
  opts = Object.assign({
      prompt: '> ',
      historySize: 1000
  }, opts || {});
  const historySize = opts.historySize;
  const histpath = opts.historyFile || path.resolve(process.cwd(), '.node_repl_history');
  const temppath = histpath + '.tmp-' + process.pid;

  return readFile(histpath, 'utf8')
  .then(hist => Array.from(new Set(hist.split(/[\n\r]+/).reverse())), err => { if (err.code === 'ENOENT') return []; else throw err; })
  .then(history => {
    var repl = REPL.start(opts);
    repl.pause();
    repl.savingHistory = true;
    var writer = fs.createWriteStream(temppath, {flags: 'a'});
    history = history.slice(0, historySize + 1);
    repl.history = history.slice(1);
    writer.write(history.reverse().join(os.EOL));
    repl.on('line', line => writer && repl.savingHistory && (line = line.trim()) && writer.write(line + os.EOL));
    repl.on('exit', () => {
      if (writer) writer.end(() => {
        rename(temppath, histpath).then(() => repl.emit('exitsafe'));
      });
      writer = null;
    });

    repl.defineCommand('cls', {
      help: 'Clear terminal window',
      action: function(name) {
        readline.cursorTo(this.outputStream, 0, 0);
        readline.clearScreenDown(this.outputStream);
        this.lineParser && this.lineParser.reset();
        if (this.clearBufferedCommand) {
          this.clearBufferedCommand();
        } else {
          this.bufferedCommand = '';
        }
        this.displayPrompt();
      }
    });

    repl.resume();

    return repl;
  });
};

function readFile(path, opts) {
  return new Promise((resolve, reject) => {
    fs.readFile(path, opts, (err, data) => {
      if (err) return reject(err);
      resolve(data);
    });
  });
};

function rename(oldPath, newPath) {
  return new Promise((resolve, reject) => {
    fs.rename(oldPath, newPath, err => {
      if (err) return reject(err);
      resolve();
    });
  });
};
