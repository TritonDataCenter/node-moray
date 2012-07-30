// Copyright 2012 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var clone = require('clone');
var fast = require('fast');
var uuid = require('node-uuid');



///--- Globals

var sprintf = util.format;



///--- API

function MorayClient(options) {
        assert.object(options, 'options');
        assert.string(options.host, 'options.host');
        assert.object(options.log, 'options.log');
        assert.number(options.port, 'options.port');

        var self = this;
        EventEmitter.call(this);


        this.client = fast.createClient(options);
        this.log = options.log.child({
                clazz: 'MorayClient',
                host: options.host,
                port: options.port
        }, true);
        this.client.on('connect', function () {
                self.emit('connect');
        });
}
util.inherits(MorayClient, EventEmitter);


MorayClient.prototype.close = function close() {
        this.client.close();
};


MorayClient.prototype.createBucket = function createBucket(b, cfg, opts, cb) {
        assert.string(b, 'bucket');
        assert.object(cfg, 'config');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var _cfg = clone(cfg);
        var client = this.client;
        var log = this.log;
        var options = {
                req_id: opts.req_id || uuid.v1()
        };
        var req;

        _cfg.pre = (cfg.pre || []).map(function (f) {
                return (f.toString());
        });
        _cfg.post = (cfg.post || []).map(function (f) {
                return (f.toString());
        });

        log.debug({
                bucket: b,
                config: _cfg,
                req_id: options.req_id
        }, 'putBucket: entered');

        req = client.rpc('createBucket', b, _cfg, options);

        req.on('end', function () {
                log.debug({
                        req_id: options.req_id
                }, 'putBucket: done');
                cb(null);
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'putBucket: failed');
                cb(err);
        });
};


MorayClient.prototype.getBucket = function getBucket(b, opts, cb) {
        assert.string(b, 'bucket');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var bucket;
        var client = this.client;
        var log = this.log;
        var options = {
                req_id: opts.req_id || uuid.v1()
        };
        var req;

        log.debug({
                bucket: b,
                req_id: options.req_id
        }, 'getBucket: entered');

        req = client.rpc('getBucket', options, b);

        req.once('message', function (obj) {
                function parseFunctor(f) {
                        var fn;
                        eval('fn = ' + f);
                        return (fn);
                }
                bucket = {
                        name: obj.name,
                        index: JSON.parse(obj.index),
                        pre: JSON.parse(obj.pre).map(parseFunctor),
                        post: JSON.parse(obj.post).map(parseFunctor),
                        mtime: new Date(obj.mtime)
                };
                log.debug({
                        req_id: options.req_id,
                        message: obj
                }, 'getBucket: bucket found');
        });

        req.once('end', function () {
                log.debug({
                        req_id: options.req_id
                }, 'getBucket: done');
                cb(null, bucket);
        });

        req.once('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'getBucket: failed');
                cb(err);
        });
};


MorayClient.prototype.updateBucket = function updateBucket(b, cfg, opts, cb) {
        assert.string(b, 'bucket');
        assert.object(cfg, 'config');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var _cfg = clone(cfg);
        var client = this.client;
        var log = this.log;
        var options = {
                req_id: opts.req_id || uuid.v1()
        };
        var req;

        _cfg.pre = (cfg.pre || []).map(function (f) {
                return (f.toString());
        });
        _cfg.post = (cfg.post || []).map(function (f) {
                return (f.toString());
        });

        log.debug({
                bucket: b,
                config: _cfg,
                req_id: options.req_id
        }, 'updateBucket: entered');

        req = client.rpc('updateBucket', b, _cfg, options);

        req.on('end', function () {
                log.debug({
                        req_id: options.req_id
                }, 'updateBucket: done');
                cb(null);
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'updateBucket: failed');
                cb(err);
        });
};


MorayClient.prototype.delBucket = function delBucket(b, opts, cb) {
        assert.string(b, 'bucket');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var client = this.client;
        var log = this.log;
        var options = {
                req_id: opts.req_id || uuid.v1()
        };
        var req;

        log.debug({
                bucket: b,
                req_id: options.req_id
        }, 'delBucket: entered');

        req = client.rpc('delBucket', b, options);

        req.on('end', function () {
                log.debug({
                        req_id: options.req_id
                }, 'delBucket: done');
                cb(null);
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'delBucket: failed');
                cb(err);
        });
};


MorayClient.prototype.putBucket = function putBucket(b, cfg, opts, cb) {
        assert.string(b, 'bucket');
        assert.object(cfg, 'config');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var log = this.log;
        var options = {
                req_id: opts.req_id || uuid.v1()
        };
        var req;
        var self = this;

        this.getBucket(b, function (err) {
                if (err) {
                        // TODO
                        // if (err.name === 'BucketNotFoundError') {
                        self.createBucket(b, cfg, opts, cb);
                } else {
                        self.updateBucket(b, cfg, opts, cb);
                }
        });
};


MorayClient.prototype.putObject = function putObject(b, k, v, opts, cb) {
        assert.string(b, 'bucket');
        assert.string(k, 'key');
        assert.object(v, 'value');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var client = this.client;
        var log = this.log;
        var options = {
                _value: JSON.stringify(v),
                req_id: opts.req_id || uuid.v1()
        };
        var req;

        log.debug({
                bucket: b,
                key: k,
                v: v,
                req_id: options.req_id
        }, 'putObject: entered');

        req = client.rpc('putObject', b, k, v, options);

        req.on('end', function () {
                log.debug({
                        req_id: options.req_id
                }, 'putObject: done');
                cb(null);
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'putObject: failed');
                cb(err);
        });
};


MorayClient.prototype.getObject = function getObject(b, k, opts, cb) {
        assert.string(b, 'bucket');
        assert.string(k, 'key');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var client = this.client;
        var log = this.log;
        var obj;
        var options = {
                noCache: opts.noCache,
                req_id: opts.req_id || uuid.v1()
        };
        var req;

        log.debug({
                bucket: b,
                key: k,
                req_id: options.req_id
        }, 'getObject: entered');

        req = client.rpc('getObject', b, k, options);

        req.on('message', function (msg) {
                obj = msg;
        });

        req.on('end', function () {
                log.debug({
                        req_id: options.req_id,
                        obj: obj
                }, 'getObject: done');
                cb(null, obj);
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'getObject: failed');
                cb(err);
        });
};


MorayClient.prototype.delObject = function delObject(b, k, opts, cb) {
        assert.string(b, 'bucket');
        assert.string(k, 'key');
        if (typeof (opts) === 'function') {
                cb = opts;
                opts = {};
        }
        assert.object(opts, 'options');
        assert.func(cb, 'callback');

        var client = this.client;
        var log = this.log;
        var options = {
                req_id: opts.req_id || uuid.v1()
        };
        var req;

        log.debug({
                bucket: b,
                key: k,
                req_id: options.req_id
        }, 'delObject: entered');

        req = client.rpc('delObject', b, k, options);

        req.on('end', function () {
                log.debug({
                        req_id: options.req_id
                }, 'delObject: done');
                cb(null);
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'delObject: failed');
                cb(err);
        });
};


MorayClient.prototype.findObjects = function findObjects(b, f, opts) {
        assert.string(b, 'bucket');
        assert.string(f, 'filter');
        if (!opts)
                opts = {};
        assert.object(opts, 'options');

        var client = this.client;
        var log = this.log;
        var options = {
                limit: opts.limit,
                offset: opts.offset,
                sort: opts.sort,
                req_id: opts.req_id || uuid.v1()
        };
        var req;
        var res = new EventEmitter();

        log.debug({
                bucket: b,
                filter: f,
                opts: opts,
                req_id: options.req_id
        }, 'findObjects: entered');

        req = client.rpc('findObjects', b, f, options);

        req.on('message', function (msg) {
                log.debug({
                        req_id: options.req_id
                }, 'findObjects: msg: %j', msg);
                res.emit('record', msg);
        });

        req.on('end', function () {
                log.debug({
                        req_id: options.req_id
                }, 'findObjects: done');
                res.removeAllListeners('data');
                res.removeAllListeners('error');
                res.emit('end');
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'findObjects: failed');
                res.removeAllListeners('data');
                res.removeAllListeners('end');
                res.emit('error', err);
        });

        return (res);
};


MorayClient.prototype.toString = function toString() {
        var str = sprintf('[object MorayClient<host=%s, port=%d]',
                          this.host, this.port);
        return (str);
};



///--- Exports

module.exports = {
        Client: MorayClient
};