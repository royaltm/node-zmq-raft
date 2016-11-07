/* 
 *  Copyright (c) 2016 Rafał Michalski <royal@yeondir.com>
 */
"use strict";

const isBuffer = Buffer.isBuffer
    , isArray  = Array.isArray;

const msgpack = require('msgpack-lite');

const bufconv      = require('../utils/bufconv')
    , intEncode    = bufconv.allocBufIntLE
    , uintEncode   = bufconv.allocBufUIntLE
    , numberEncode = bufconv.allocBufNumberLE
    , boolEncode   = bufconv.boolToBuffer
    , readBool     = bufconv.bufferToBool
    , readUInt     = bufconv.readBufUIntLE
    , readInt      = bufconv.readBufIntLE
    , readNumber   = bufconv.readBufNumberLE

const { ZmqSocket } = require('../utils/zmqsocket');

const debug = require('debug')('zmq-raft:frames-protocol');

class FramesProtocol {
  /**
   * Create FramesProtocol
   *
   * `options` may be one of:
   *
   * - `name` {string}: used for protocol identification in error messages
   * - `required` {number|Array}: how many frame elements are required,
   *    if array first element for request and the second for response
   * - `extraArgs` {boolean|Array}: pass additional args exceedeing schema
   *    if array first element for request and the second for response
   *
   * @param {Array} requestSchema
   * @param {Array} responseSchema
   * @param {Object} options
   * @return {FramesProtocol}
  **/
  constructor(requestSchema, responseSchema, options) {
    if (!isArray(requestSchema)) throw new TypeError('FramesProtocol error: requestSchema must be an array');
    if (!isArray(responseSchema)) throw new TypeError('FramesProtocol error: responseSchema must be an array');
    options || (options = {});
    var required = options.required;
    if (!isArray(required)) required = [required, required];
    var extraArgs = options.extraArgs;
    if (!isArray(extraArgs)) extraArgs = [extraArgs, extraArgs];
    this.name = String(options.name || 'unnamed');
    this.encodeRequest = createSchemaEncoder(requestSchema, required[0]|0, !!extraArgs[0]);
    this.decodeRequest = createSchemaDecoder(requestSchema, required[0]|0, !!extraArgs[0]);
    this.encodeResponse = createSchemaEncoder(responseSchema, required[1]|0, !!extraArgs[1]);
    this.decodeResponse = createSchemaDecoder(responseSchema, required[1]|0, !!extraArgs[1]);
  }

  toString() {
    return this.name + ' protocol';
  }

  /**
   * Creates specialized request function for RpcSocket instance
   *
   * @param {RpcSocket} rpc
   * @return {Function}
  **/
  createRequestFunctionFor(rpc) {
    if (!rpc || 'object' !== typeof rpc) throw new TypeError(`${this.name}: rpc must be an object`);
    if ('function' !== typeof rpc.request) throw new TypeError(`${this.name}: rpc must have request method`);
    const encodeRequest = this.encodeRequest;
    const decodeResponse = this.decodeResponse;
    /* create unbound request function */
    return (req, opt) => rpc.request(encodeRequest(req), opt).then(resp => decodeResponse(resp));
  }

  /**
   * Creates specialized router message listener and attaches it to 'frames' event
   *
   * @param {zmq.Socket} router
   * @param {Function} handler(reply{Function}, decodedArgs{Array})
   * @param {Object} [context] handler's calling context
   * @return {Function} listener for removing from events
  **/
  createRouterMessageListener(router, handler, context) {
    if (!(router instanceof ZmqSocket)) throw new TypeError(`${this.name}: router must be an instance of ZmqSocket`);

    const name = this.name;
    const decodeRequest = this.decodeRequest;
    const encodeResponse = this.encodeResponse;

    var listener = (args) => {
      var ident = args.shift(), requestId = args.shift();

      try {
        if (ident === undefined) throw new Error("router received message without ident frame")
        if (requestId === undefined) throw new Error("router received message without requestId frame")
        args = decodeRequest(args);
      } catch(err) {
        /* prevent easy DDoS */
        debug('ERROR decoding %s: %s', this, err);
        return;
      }

      var payload = [ident, requestId];

      var reply = (args) => {
        router.send(payload.concat(encodeResponse(args)));
      };
      reply.requestId = requestId;
      reply.ident = ident;

      handler.call(context, reply, args);
    };

    router.on('frames', listener);

    return listener;
  }

  /**
   * Creates specialized subscriber message listener and attaches it to 'frames' event
   *
   * @param {zmq.Socket} sub
   * @param {Function} handler(decodedArgs{Array})
   * @param {Object} [context] handler's calling context
   * @return {Function} listener for removing from events
  **/
  createSubMessageListener(sub, handler, context) {
    if (!(sub instanceof ZmqSocket)) throw new TypeError(`${this.name}: sub must be an instance of ZmqSocket`);

    const name = this.name;
    const decodeRequest = this.decodeRequest;

    var listener = (args) => {
      try {
        args = decodeRequest(args);
      } catch(err) {
        /* prevent easy DDoS */
        debug('ERROR decoding %s: %s', this, err);
        return;
      }

      handler.call(context, args);
    };

    sub.on('frames', listener);

    return listener;
  }

  /**
   * Creates specialized fan out send function for (preferably ZMQ_PUB) socket instance
   *
   * @param {zmq.socket} pub
   * @return {Function}
  **/
  createSendFunctionFor(pub) {
    if (!(pub instanceof ZmqSocket)) throw new TypeError(`${this.name}: pub must be an instance of ZmqSocket`);
    const encodeRequest = this.encodeRequest;
    /* create unbound request function */
    return (req, flags) => pub.send(encodeRequest(req), flags);
  }

}

module.exports = exports = FramesProtocol;

const emptyFrameCoder = () => [];
const emptyFrameEncoderArgs = (args) => isArray(args) ? args : args === undefined ? [] : [args];

const encoderBodies = {
  'string':   'buf("string"===typeof aN ? aN : String(aN))',
  'utf8':     'buf("string"===typeof aN ? aN : String(aN), "utf8")',
  'utf-8':    'buf("string"===typeof aN ? aN : String(aN), "utf8")',
  'hex':      'buf("string"===typeof aN ? aN : String(aN), "hex")',
  'base64':   'buf("string"===typeof aN ? aN : String(aN), "base64")',
  'binary':   'buf("string"===typeof aN ? aN : String(aN), "binary")',
  'latin1':   'buf("string"===typeof aN ? aN : String(aN), "latin1")',
  'utf16le':  'buf("string"===typeof aN ? aN : String(aN), "utf16le")',
  'ucs2':     'buf("string"===typeof aN ? aN : String(aN), "ucs2")',
  'ascii':    'buf("string"===typeof aN ? aN : String(aN), "ascii")',
  'bool':     'boolEncode(aN)',
  'boolean':  'boolEncode(aN)',
  'unsigned': 'uintEncode(aN)',
  'uint':     'uintEncode(aN)',
  'int':      'intEncode(aN)',
  'integer':  'intEncode(aN)',
  'number':   'numberEncode(aN)',
  'object':   'encode(aN)',
  'json':     'encode(aN)',
  'buffer':   'buf(aN)'
};

function createSchemaEncoder(schema, required, extraArgs) {
  if (required > schema.length) throw new TypeError('encoder schema error: too much required arguments');
  if (schema.length === 0) {
    return extraArgs ? emptyFrameEncoderArgs : emptyFrameCoder;
  }
  var extraBody = '';
  var handleSingleArg;
  var components = schema.map((type, index) => {
    var body = encoderBodies[type];
    if (!body) throw new TypeError('encoder schema error: unknown type: ' + type);
    body = 'aN instanceof Buffer ? aN : ' + body;
    if (index === 0) {
      handleSingleArg = body.replace(/aN/g, 'args');
    }
    if (index >= required) {
      body = `if (aN === undefined) return frms; frms.push(${body});`;
    }
    return body.replace(/aN/g, 'a' + index);
  });

  if (extraArgs) {
    extraBody = 'if (args.length !== size) return frms.concat(args.slice(size)); ';
  }

  if (required > 1) {
    handleSingleArg = 'throw new Error("encode frames error: not enough arguments");'
  }
  else if (required === 1) {
    handleSingleArg = `if (args === undefined) throw new Error("encode frames error: not enough arguments");
  return [${handleSingleArg}];`;
  }
  else handleSingleArg = `return args === undefined ? [] : [${handleSingleArg}];`;

  return new Function(
    'size',
    'required',
    'isArray',
    'buf',
    'encode',
    'intEncode',
    'uintEncode',
    'numberEncode',
    'boolEncode',
    `return (args) => {
  if (!isArray(args)) {
    ${handleSingleArg}
  }
  if (args.length < required) throw new Error("encode frames error: not enough arguments");
  var ${schema.map((_,i) => 'a' + i + ' = args[' + i + ']').join(',')}, frms = [${components.slice(0, required).join(',')}];
  ${components.slice(required).join('\n  ')}
  ${extraBody}return frms;
}`)(
      schema.length,
      required,
      isArray,
      Buffer.from,
      msgpack.encode,
      intEncode,
      uintEncode,
      numberEncode,
      boolEncode);
  }

const emptyFrameDecoderArgs = (args) => args;

const decoderBodies = {
  'string':   'aN.toString()',
  'utf8':     'aN.toString("utf8")',
  'utf-8':    'aN.toString("utf8")',
  'hex':      'aN.toString("hex")',
  'base64':   'aN.toString("base64")',
  'binary':   'aN.toString("binary")',
  'latin1':   'aN.toString("latin1")',
  'utf16le':  'aN.toString("utf16le")',
  'ucs2':     'aN.toString("ucs2")',
  'ascii':    'aN.toString("ascii")',
  'bool':     'readBool(aN)',
  'boolean':  'readBool(aN)',
  'unsigned': 'readUInt(aN)',
  'uint':     'readUInt(aN)',
  'int':      'readInt(aN)',
  'integer':  'readInt(aN)',
  'number':   'readNumber(aN)',
  'object':   'decode(aN)',
  'json':     'decode(aN)',
  'buffer':   'aN'
};

function createSchemaDecoder(schema, required, extraArgs) {
  if (required > schema.length) throw new TypeError('decoder schema error: too much required arguments');
  if (schema.length === 0) {
    return extraArgs ? emptyFrameDecoderArgs : emptyFrameCoder;
  }
  var extraBody = '';
  var components = schema.map((type, index) => {
    var body = decoderBodies[type];
    if (!body) throw new TypeError('encoder schema error: unknown type: ' + type);
    if (index >= required) {
      body = `aN === undefined ? undefined : ${body}`;
    }
    return body.replace(/aN/g, 'a' + index);
  });

  if (extraArgs) {
    extraBody = 'if (frms.length !== size) return args.concat(frms.slice(size)); ';
  }

  return new Function(
    'size',
    'required',
    'decode',
    'readInt',
    'readUInt',
    'readNumber',
    'readBool',
    `return (frms) => {
  if (frms.length < required) throw new Error("decode frames error: not enough frames");
  var ${schema.map((_,i) => 'a' + i + ' = frms[' + i + ']').join(',')}, args = [${components.join(',')}];
  ${extraBody}return args;
}`)(
      schema.length,
      required,
      msgpack.decode,
      readInt,
      readUInt,
      readNumber,
      readBool);
}
