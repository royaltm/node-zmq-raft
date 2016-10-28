/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

/**
 * promise resolving async callback generator for wrapping callback style api
 *
 * Example:
 *
 *     parallel((cb1, cb2) => {
 *      someAsyncJob(arg1, cb1);
 *      someOtherAsyncJob(arg1, cb2);
 *     }).then(results => { }, err => { })
 *
 * @return {Promise}
 */
module.exports = function parallel(callback) {
  var count = callback.length;
  var results = new Array(count);
  var cbs = [];
  var done = 0;
  var promise = new Promise((resolve, reject) => {
    times(count, index => {
      cbs.push(function(err, res) {
        if (err != null) {
          reject(err);
        }
        else {
          results[index] = res;
          if (++done === count) resolve(results);
        }
      });
    });
  });
  callback.apply(null, cbs);
  return promise;
};

function times(n, callback) {
  for(var i = 0; i < n; ++i) callback(i);
}
