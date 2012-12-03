// Copyright 2012 Joyent, Inc.  All rights reserved.

var dns = require('dns');
var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');
var clone = require('clone');
var fast = require('fast');
var once = require('once');
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

        this.host = options.host;
        this.client = fast.createClient(clone(options));
        this.log = options.log.child({
                component: 'MorayClient',
                host: options.host,
                port: options.port
        }, true);
        this.noCache = options.noCache || self.noCache;
        this.port = options.port;

        this.client.on('connect', function () {
                // Update the client address so .toString() shows us what
                // this client is connected to, instead of the usual DNS
                // name
                self.host = self.client.conn.remoteAddress;
                self.emit('connect');
        });
        this.client.on('close', self.emit.bind(self, 'close'));
        this.client.on('connectAttempt',
                       self.emit.bind(self, 'connectAttempt'));
        this.client.on('error', self.emit.bind(self, 'error'));

        this.__defineGetter__('connection', function getConnection() {
                return (self.client.conn);
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

        cb = once(cb);
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
        }, 'createBucket: entered');

        req = client.rpc('createBucket', b, _cfg, options);

        req.on('end', function () {
                log.debug({
                        req_id: options.req_id
                }, 'createBucket: done');
                cb(null);
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'createBucket: failed');
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

        cb = once(cb);

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
                        /* jsl:ignore */
                        eval('fn = ' + f);
                        /* jsl:end */
                        return (fn);
                }
                bucket = {
                        name: obj.name,
                        index: JSON.parse(obj.index),
                        pre: JSON.parse(obj.pre).map(parseFunctor),
                        post: JSON.parse(obj.post).map(parseFunctor),
                        options: JSON.parse(obj.options),
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

        cb = once(cb);
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

        cb = once(cb);
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

        cb = once(cb);
        var options = {
                req_id: opts.req_id || uuid.v1()
        };
        var self = this;

        this.getBucket(b, options, function (err) {
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

        cb = once(cb);
        var client = this.client;
        var log = this.log;
        var meta;
        var options = {
                _value: JSON.stringify(v),
                etag: (opts.etag || opts._etag),
                req_id: opts.req_id || uuid.v1(),
                headers: opts.headers || {}
        };
        var req;

        log.debug({
                bucket: b,
                key: k,
                v: v,
                etag: options.etag,
                req_id: options.req_id
        }, 'putObject: entered');

        req = client.rpc('putObject', b, k, v, options);

        req.once('message', function (msg) {
                meta = msg;
        });

        req.once('end', function () {
                log.debug({
                        req_id: options.req_id
                }, 'putObject: done');
                cb(null, meta || {});
        });

        req.once('error', function (err) {
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

        cb = once(cb);
        var self = this;
        var client = this.client;
        var log = this.log;
        var obj;
        var options = {
                noCache: opts.noCache || self.noCache,
                req_id: opts.req_id || uuid.v1(),
                headers: opts.headers || {}
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

        cb = once(cb);
        var client = this.client;
        var log = this.log;
        var options = {
                etag: (opts.etag || opts._etag),
                req_id: opts.req_id || uuid.v1(),
                headers: opts.headers || {}
        };
        var req;

        log.debug({
                bucket: b,
                key: k,
                etag: options.etag,
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
                req_id: opts.req_id || uuid.v1(),
                headers: opts.headers || {}
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
                res.removeAllListeners('record');
                res.removeAllListeners('error');
                res.emit('end');
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'findObjects: failed');
                res.removeAllListeners('record');
                res.removeAllListeners('end');
                res.emit('error', err);
        });

        return (res);
};
MorayClient.prototype.find = MorayClient.prototype.findObjects;


MorayClient.prototype.ping = function ping(opts, cb) {
        if (!opts)
                opts = {};

        assert.object(opts, 'options');
        assert.optionalNumber(opts.timeout, 'options.timeout');
        assert.func(cb, 'callback');

        cb = once(cb);
        var client = this.client;
        var log = this.log;
        var options = {
                deep: opts.deep || false,
                req_id: opts.req_id || uuid.v1()
        };
        var req;
        var t;

        log.debug({
                deep: opts.deep,
                req_id: options.req_id
        }, 'ping: entered');

        req = client.rpc('ping', options);

        req.once('end', function () {
                clearTimeout(t);
                log.debug({
                        req_id: options.req_id
                }, 'ping: done');
                cb(null);
        });

        req.once('error', function (err) {
                clearTimeout(t);
                log.debug({
                        err: err,
                        req_id: options.req_id
                }, 'ping: failed');
                cb(err);
        });

        t = setTimeout(function () {
                cb(new Error('ping: timeout'));
        }, (opts.timeout || 1000));
};


MorayClient.prototype.sql = function sql(stmt, vals, opts) {
        switch (arguments.length) {
        case 0:
                throw new TypeError('statement (String) required');
        case 1:
                assert.string(stmt, 'statement');
                vals = [];
                opts = {};
                break;
        case 2:
                assert.string(stmt, 'statement');
                if (!Array.isArray(vals)) {
                        assert.object(vals, 'options');
                        opts = vals;
                        vals = [];
                } else {
                        opts = {};
                }
                break;
        case 3:
                assert.string(stmt, 'statement');
                assert.ok(Array.isArray(vals));
                assert.object(opts, 'options');
                break;
        default:
                throw new Error('too many arguments');
        }

        var client = this.client;
        var log = this.log;
        var req;
        var res = new EventEmitter();

        log.debug({
                statement: stmt,
                values: vals,
                options: opts,
                req_id: opts.req_id
        }, 'sql: entered');

        req = client.rpc('sql', stmt, vals, opts);

        req.on('message', function (msg) {
                log.debug({
                        req_id: opts.req_id
                }, 'sql: msg: %j', msg);
                res.emit('record', msg);
        });

        req.on('end', function () {
                log.debug({
                        req_id: opts.req_id
                }, 'sql: done');
                res.removeAllListeners('res');
                res.removeAllListeners('error');
                res.emit('end');
        });

        req.on('error', function (err) {
                log.debug({
                        err: err,
                        req_id: opts.req_id
                }, 'sql: failed');
                res.removeAllListeners('data');
                res.removeAllListeners('end');
                res.emit('error', err);
        });

        return (res);
};


MorayClient.prototype.toString = function toString() {
        var str = sprintf('[object MorayClient<host=%s, client=%s>]',
                          this.host, this.client.toString());
        return (str);
};



///--- Exports

module.exports = {
        Client: MorayClient
};
