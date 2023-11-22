/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

/*
  Promisify only what we need in a way we need.
*/

const {
  access,
  close,
  constants: { O_RDONLY, O_DIRECTORY },
  fdatasync,
  fstat,
  fsync,
  ftruncate,
  open,
  link,
  read,
  readFile,
  readdir,
  rename,
  stat,
  unlink,
  write,
  writeFile
} = require('fs');

const { dirname } = require('path');
const { mkdirp } = require('mkdirp');
const parallel = require('../utils/parallel');

const debug = require('debug')('zmq-raft:fsutils');

if (!O_DIRECTORY) debug("OS does not support directory syncing with fsync");

/* resolves to dirfd only on supported OS'es, otherwise a no-op resolves to null */
var openDirAsync = exports.openDir = function(path) {
  if (!O_DIRECTORY) return Promise.resolve(null);
  return openAsync(path, O_DIRECTORY|O_RDONLY);
};

/* close directory opened with openDir for syncing */
var closeDirAsync = exports.closeDir = function(dirfd) {
  if (!O_DIRECTORY) return Promise.resolve();
  return closeAsync(dirfd);
};

exports.fsyncDirFileCloseDir = function(dirfd, fd) {
  return parallel((cb1, cb2) => {
    fsync(fd, cb1); /* ensure file durability, well, sort of */
    if (O_DIRECTORY) {
      /* ensure directory durability */
      /* well, too bad for windows this is unreplaceable, let's hope for FlushFileBuffers to sync it all */
      fsync(dirfd, cb2);
    }
    else {
      cb2();
    }
  })
  .then(() => O_DIRECTORY && closeAsync(dirfd));
};

exports.renameSyncDir = function(oldPath, newPath) {
  if (!O_DIRECTORY) return renameAsync(oldPath, newPath);
  var oldDir = dirname(oldPath);
  var newDir = dirname(newPath);
  if (oldDir === newDir) {
    return openAsync(oldDir, O_DIRECTORY|O_RDONLY)
    .then(dirfd => {
      return renameAsync(oldPath, newPath)
      .then(() => fsyncAsync(dirfd)).then(() => closeAsync(dirfd))
    });
  } else {
    return Promise.all([
      openAsync(oldDir, O_DIRECTORY|O_RDONLY),
      openAsync(newDir, O_DIRECTORY|O_RDONLY)
    ]).then(dirfds => renameAsync(oldPath, newPath)
      .then(() => Promise.all([
        fsyncAsync(dirfds[0]).then(() => closeAsync(dirfds[0])),
        fsyncAsync(dirfds[1]).then(() => closeAsync(dirfds[1]))
      ]))
    );
  }
};

exports.write = function(fd, buffer, offset, length, position) {
  return new Promise((resolve, reject) => {
    write(fd, buffer, offset, length, position, (err, bytesWritten) => {
      if (err) return reject(err);
      if (bytesWritten !== length) return reject(new Error("bytesWritten mismatch"));
      resolve(bytesWritten);
    });
  });
};

exports.read = function(fd, buffer, offset, length, position) {
  return new Promise((resolve, reject) => {
    read(fd, buffer, offset, length, position, (err, bytesRead) => {
      if (err) return reject(err);
      if (bytesRead !== length) return reject(new Error("bytesRead <> expected"));
      resolve(bytesRead);
    });
  });
};

var openAsync = exports.open = function(path, flags, mode) {
  return new Promise((resolve, reject) => {
    open(path, flags, mode, (err, fd) => {
      if (err) return reject(err);
      resolve(fd);
    });
  });
};

var closeAsync = exports.close = function(fd) {
  return new Promise((resolve, reject) => {
    close(fd, err => {
      if (err) return reject(err);
      resolve();
    });
  });
};

exports.ftruncate = function(fd, length) {
  return new Promise((resolve, reject) => {
    ftruncate(fd, length, err => {
      if (err) return reject(err);
      resolve();
    });
  });
};

exports.fdatasync = function(fd) {
  return new Promise((resolve, reject) => {
    fdatasync(fd, err => {
      if (err) return reject(err);
      resolve();
    });
  });
};

var fsyncAsync = exports.fsync = function(fd) {
  return new Promise((resolve, reject) => {
    fsync(fd, err => {
      if (err) return reject(err);
      resolve();
    });
  });
};

exports.fstat = function(fd) {
  return new Promise((resolve, reject) => {
    fstat(fd, (err, stat) => {
      if (err) return reject(err);
      resolve(stat);
    });
  });
};

exports.stat = function(path) {
  return new Promise((resolve, reject) => {
    stat(path, (err, stat) => {
      if (err) return reject(err);
      resolve(stat);
    });
  });
};

var renameAsync = exports.rename = function(oldPath, newPath) {
  return new Promise((resolve, reject) => {
    rename(oldPath, newPath, err => {
      if (err) return reject(err);
      resolve();
    });
  });  
};

exports.link = function(srcPath, dstPath) {
  return new Promise((resolve, reject) => {
    link(srcPath, dstPath, err => {
      if (err) return reject(err);
      resolve();
    });
  });  
};

exports.mkdirp = mkdirp;

exports.readdir = function(path, opts) {
  return new Promise((resolve, reject) => {
    readdir(path, opts, (err, files) => {
      if (err) return reject(err);
      resolve(files);
    });
  });
};

exports.readFile = function(path, opts) {
  return new Promise((resolve, reject) => {
    readFile(path, opts, (err, data) => {
      if (err) return reject(err);
      resolve(data);
    });
  });
};

exports.writeFile = function(path, data, opts, callback) {
  return new Promise((resolve, reject) => {
    writeFile(path, data, opts, (err) => {
      if (err) return reject(err);
      resolve();
    });
  });
};

exports.unlink = function(path) {
  return new Promise((resolve, reject) => {
    unlink(path, err => {
      if (err) return reject(err);
      resolve();
    });
  });
};

exports.access = function(path, mode) {
  return new Promise((resolve, reject) => {
    access(path, mode, err => {
      if (err) return reject(err);
      resolve();
    });
  });
};
