/* 
 *  Copyright (c) 2017-2018 Rafał Michalski <royal@yeondir.com>
 *  License: LGPL
 */
"use strict";

const test = require('tap').test;
const path = require('path');
const { readConfig } = require('../../lib/utils/config');
const { createOptions } = require('../../lib/server/builder');

const configBase = path.join(__dirname, 'configs', 'example');

test('should give defaults when no config file is provided', t => {
  t.plan(4);
  return readConfig('')
  .then(config => {
    t.deepEquals(config, createOptions());
    return readConfig('', 'raft')
  })
  .then(config => {
    t.deepEquals(config, createOptions());
    return readConfig('foo.xyz');
  })
  .catch(err => {
    t.strictSame(err, new Error("Unrecognized config file type. Use one of: yaml, json, hjson, toml, js"));
    return readConfig('', 'foo.bar');
  })
  .catch(err => {
    t.strictSame(err, new Error("There is no such configuration namespace: foo.bar"));
  })
  .catch(t.threw);
});

test('should merge js config file', t => {
  t.plan(3);
  return readConfig(configBase + '.js')
  .then(config => {
    t.deepEquals(config, createOptions({id: 'example-js', secret: 'foo-js'}));
    return readConfig(configBase + '.js', 'other.cfg');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'other-js', secret: 'bar-js'}));
    return readConfig(configBase + '.js', '');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'root-js', secret: 'baz-js',
      raft: { id: 'example-js', secret: 'foo-js' },
      other: { cfg: { id: 'other-js', secret: 'bar-js' } }
    }));
  })
  .catch(t.threw);
});

test('should merge json config file', t => {
  t.plan(3);
  return readConfig(configBase + '.json')
  .then(config => {
    t.deepEquals(config, createOptions({id: 'example-json', secret: 'foo-json'}));
    return readConfig(configBase + '.json', 'other.cfg');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'other-json', secret: 'bar-json'}));
    return readConfig(configBase + '.json', '');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'root-json', secret: 'baz-json',
      raft: { id: 'example-json', secret: 'foo-json' },
      other: { cfg: { id: 'other-json', secret: 'bar-json' } }
    }));
  })
  .catch(t.threw);
});

test('should merge hjson config file', t => {
  t.plan(3);
  return readConfig(configBase + '.hjson')
  .then(config => {
    t.deepEquals(config, createOptions({id: 'example-hjson', secret: 'foo-hjson'}));
    return readConfig(configBase + '.hjson', 'other.cfg');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'other-hjson', secret: 'bar-hjson'}));
    return readConfig(configBase + '.hjson', '');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'root-hjson', secret: 'baz-hjson',
      raft: { id: 'example-hjson', secret: 'foo-hjson' },
      other: { cfg: { id: 'other-hjson', secret: 'bar-hjson' } }
    }));
  })
  .catch(t.threw);
});

test('should merge yaml config file', t => {
  t.plan(3);
  return readConfig(configBase + '.yaml')
  .then(config => {
    t.deepEquals(config, createOptions({id: 'example-yaml', secret: 'foo-yaml'}));
    return readConfig(configBase + '.yaml', 'other.cfg');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'other-yaml', secret: 'bar-yaml'}));
    return readConfig(configBase + '.yaml', '');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'root-yaml', secret: 'baz-yaml',
      raft: { id: 'example-yaml', secret: 'foo-yaml' },
      other: { cfg: { id: 'other-yaml', secret: 'bar-yaml' } }
    }));
  })
  .catch(t.threw);
});

test('should merge toml config file', t => {
  t.plan(3);
  return readConfig(configBase + '.toml')
  .then(config => {
    t.deepEquals(config, createOptions({id: 'example-toml', secret: 'foo-toml'}));
    return readConfig(configBase + '.toml', 'other.cfg');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'other-toml', secret: 'bar-toml'}));
    return readConfig(configBase + '.toml', '');
  })
  .then(config => {
    t.deepEquals(config, createOptions({id: 'root-toml', secret: 'baz-toml',
      raft: { id: 'example-toml', secret: 'foo-toml' },
      other: { cfg: { id: 'other-toml', secret: 'bar-toml' } }
    }));
  })
  .catch(t.threw);
});
