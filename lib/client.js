// Copyright 2013 Joyent, Inc.  All rights reserved.

var domain = require('domain');
var EventEmitter = require('events').EventEmitter;
var net = require('net');
var util = require('util');

var assert = require('assert-plus');
var fast = require('fast');
var libuuid = require('libuuid');
var once = require('once');

var buckets = require('./buckets');
var dns = require('./dns');
var connection = require('./connection');
var objects = require('./objects');
var tokens = require('./tokens');
var utils = require('./utils');



///--- Globals

var sprintf = util.format;
var clone = utils.clone;



///--- Helpers

// Super fugly way to return symmetric difference of two arrays
function diffArray(a, b) {
    var arr = [];

    Object.keys(a).forEach(function (k) {
        if (b.indexOf(a[k]) === -1 && arr.indexOf(a[k]) === -1)
            arr.push(a[k]);
    });

    Object.keys(b).forEach(function (k) {
        if (a.indexOf(b[k]) === -1 && arr.indexOf(b[k]) === -1)
            arr.push(b[k]);
    });

    return (arr);
}



///--- API

function ping(client, options, callback) {
    assert.object(client, 'client');
    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.optionalNumber(options.timeout, 'options.timeout');
    assert.func(callback, 'callback');

    callback = once(callback);

    var opts = {
        deep: options.deep || false,
        req_id: options.req_id || libuuid.create()
    };
    var log = utils.childLogger(client, opts);
    var req;
    var t;

    log.debug(opts, 'ping: entered');

    function done(err) {
        clearTimeout(t);
        req.removeAllListeners('end');
        req.removeAllListeners('error');

        log.debug({
            err: err,
            req_id: opts.req_id
        }, 'ping: %s', err ? 'failed' : 'done');

        callback(err || null);
    }

    req = client.rpc('ping', opts);
    req.once('end', done);
    req.once('error', done);
    t = setTimeout(function onTimeout() {
        done(new Error('ping: timeout'));
    }, (options.timeout || 1000));
}


function sql(client, statement, values, options) {
    assert.object(client, 'client');
    assert.string(statement, 'statement');
    assert.ok(Array.isArray(values));
    assert.object(options, 'options');

    var opts = {
        req_id: options.req_id || libuuid.create()
    };
    var log = utils.childLogger(client, opts);
    var req = client.rpc('sql', statement, values, opts);
    var res = new EventEmitter();

    log.debug({
        statement: statement,
        values: values
    }, 'sql: entered');

    req.on('message', function (msg) {
        log.debug('sql: msg: %j', msg);
        res.emit('record', msg);
    });

    req.once('end', function () {
        log.debug('sql: done');
        res.removeAllListeners('res');
        res.removeAllListeners('error');
        res.emit('end');
    });

    req.once('error', function (err) {
        log.debug({
            err: err,
            req_id: opts.req_id
        }, 'sql: failed');
        res.removeAllListeners('data');
        res.removeAllListeners('end');
        res.emit('error', err);
    });

    return (res);
}


///--- API

/**
 * Constructor for the moray client.
 *
 * This client, when given a DNS name will create a ring of TCP connections to
 * each IP in the DNS record (RR).  Each operation will then be round-robined
 * across both remote hosts and local connections, ensuring that any given
 * agent talking to moray is driving load equally across all available moray
 * CPUs.
 *
 * In addition, this client will periodically poll DNS looking for "dropped" or
 * added servers, and adjust the pools as apporpriate.  Note that right now this
 * is highly invasive, as the client just blatantly closes all old connections
 * and replaces with new ones, as it's assumed this is an infrequent event.
 *
 * A sample invocation:
 *
 *         var client = moray.createClient({
 *               connectTimeout: 1000,  // default 2s
 *               dns: {
 *                       checkInterval: 2000,  // default 30s
 *                       resolvers: ['10.99.99.201'], //def  /etc/resolv.conf
 *                       timeout: 500 // default 1s
 *               },
 *               log: bunyan.createLogger({
 *                       name: 'moray_client',
 *                       level: process.env.LOG_LEVEL || 'debug',
 *                       stream: process.stdout
 *               }),
 *               max: 4, // default 10
 *               retry: {retries: 2}, // defaults 3
 *               url: process.env.MORAY_URL || 'tcp://127.0.0.1:2020'
 *       });
 *
 */
function MorayClient(options) {
    assert.object(options, 'options');
    assert.optionalNumber(options.checkInterval, 'options.checkInterval');
    assert.optionalNumber(options.connectTimeout, 'options.connectTimeout');
    assert.optionalObject(options.dns, 'options.dns');
    options.dns = options.dns || {};
    assert.optionalArrayOfString(options.dns.resolvers,
                                 'options.dns.resolvers');
    assert.optionalNumber(options.dns.timeout, 'options.dns.timeout');
    assert.string(options.host, 'options.host');
    assert.object(options.log, 'options.log');
    assert.optionalNumber(options.maxConnections, 'options.maxConnections');
    assert.optionalNumber(options.maxIdleTime, 'options.maxIdleTime');
    assert.optionalNumber(options.pingTimeout, 'options.pingTimeout');
    assert.number(options.port, 'options.port');
    assert.optionalObject(options.retry, 'options.retry');

    var self = this;
    EventEmitter.call(this);

    this.connectTimeout = options.connectTimeout || 2000;
    this.host = options.host;
    this.log = options.log.child({
        component: 'MorayClient',
        host: options.host,
        port: options.port
    }, true);
    this.maxConnections = options.maxConnections || 10;
    this.noCache = options.noCache || false;
    this.port = options.port;

    var log = this.log;
    var dns_opts = {
        domain: options.host,
        log: options.log,
        resolvers: options.dns.resolvers,
        retry: options.retry || {retries: 3},
        timeout: options.dns.timeout || 1000
    };
    var pool_opts = {
        connectTimeout: self.connectTimeout,
        dns: dns_opts,
        host: self.host,
        log: self.log,
        max: self.maxConnections,
        port: self.port,
        retry: options.retry || {
            maxTimeout: 10000,
            retries: Infinity
        }
    };

    var d = domain.create();
    d.on('error', this.emit.bind(this, 'error'));
    d.run(function manageConnectionPool() {
        var freq = options.dns.checkInterval || 60000;
        var schedDNSCheck = setTimeout.bind(null, dns_check, freq);

        // Here we naively just destroy the old one and
        // replace with the new one. Suboptimal, but
        // it doesn't happen that often
        function cb(err, pool) {
            if (err) {
                log.error({
                    err: err
                }, 'failed to reconnect');
            } else {
                self.pool.close();
                self.pool = pool;
            }
            self.timer = schedDNSCheck();
        }

        function dns_check() {
            log.debug('rechecking %s in DNS', dns_opts.domain);
            dns.resolve(dns_opts, function (dns_err, ips) {
                if (dns_err) {
                    log.error(dns_err, 'DNS lookup failed');
                    self.timer = schedDNSCheck();
                    return;
                }

                var poolIps = Object.keys(self.pool.table);
                if (diffArray(ips, poolIps).length === 0) {
                    log.debug({
                        domain: dns_opts.domain,
                        ips: ips
                    }, 'no change to in DNS');
                    self.timer = schedDNSCheck();
                    return;
                }

                log.info({
                    domain: dns_opts.domain,
                    ips: ips
                }, 'DNS changed, replacing connections');
                connection.createConnectionPool(pool_opts, cb);
            });
        }

        connection.createConnectionPool(pool_opts, function (err, p) {
            if (err) {
                self.emit('error', err);
                return;
            }

            self.pool = p;
            self.emit('connect');
            if (!net.isIP(self.host))
                self.timer = schedDNSCheck();
        });
    });
}
util.inherits(MorayClient, EventEmitter);


/**
 * Shuts down all connections and closes this client down.
 */
MorayClient.prototype.close = function close() {
    clearTimeout(this.timer);
    this.pool.once('close', this.emit.bind(this, 'close'));
    this.pool.close();
};


/**
 * Creates a Bucket
 *
 * `cfg` allows you to pass in index information, as well as pre/post triggers.
 * See https://mo.joyent.com/docs/moray/master/#CreateBucket for more info.
 *
 * @param {String} b    - Bucket name
 * @param {Object} cfg  - configuration
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.createBucket = function createBucket(b, cfg, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    var client = this.getClient(cb);
    if (client)
        buckets.createBucket(client, b, cfg, opts, cb);
};


/**
 * Fetches a Bucket
 *
 * See https://mo.joyent.com/docs/moray/master/#GetBucket for more info.
 *
 * @param {String} b    - Bucket name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.getBucket = function getBucket(b, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    var client = this.getClient(cb);
    if (client)
        buckets.getBucket(client, b, opts, cb);
};


/**
 * Updates an existing Bucket
 *
 * `cfg` allows you to pass in index information, as well as pre/post triggers.
 * See https://mo.joyent.com/docs/moray/master/#UpdateBucket for more info.
 *
 * @param {String} b    - Bucket name
 * @param {Object} cfg  - configuration
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.updateBucket = function updateBucket(b, cfg, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    var client = this.getClient(cb);
    if (client)
        buckets.updateBucket(client, b, cfg, opts, cb);
};


/**
 * Deletes a Bucket
 *
 * See https://mo.joyent.com/docs/moray/master/#DeleteBucket for more info.
 *
 * @param {String} b    - Bucket name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.deleteBucket = function deleteBucket(b, opts, cb) {
    assert.string(b, 'bucket');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var client = this.getClient(cb);
    if (client)
        buckets.deleteBucket(client, b, opts, cb);
};
MorayClient.prototype.delBucket = MorayClient.prototype.deleteBucket;


/**
 * Creates or replaces a Bucket.
 *
 * Note that this is actually just a client shim, and simply calls
 * get, followed by create || update.  This is not transactional,
 * and there are races, so you probably just want to call this once
 * at startup in your code.
 *
 * @param {String} b    - Bucket name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.putBucket = function putBucket(b, cfg, opts, cb) {
    assert.string(b, 'bucket');
    assert.object(cfg, 'config');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var client = this.getClient(cb);
    if (client)
        buckets.putBucket(client, b, cfg, opts, cb);
};


/**
 * Idempotently Creates or Replaces an Object.
 *
 * See https://mo.joyent.com/docs/moray/master/#PutObject for more info.
 *
 * @param {String} b    - Bucket name
 * @param {String} k    - Key name
 * @param {Object} v    - Value
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
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

    var client = this.getClient(cb);
    if (client)
        objects.putObject(client, b, k, v, opts, cb);
};


/**
 * Fetches an Object
 *
 * See https://mo.joyent.com/docs/moray/master/#GetObject for more info.
 *
 * @param {String} b    - Bucket name
 * @param {String} k    - Key name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.getObject = function getObject(b, k, opts, cb) {
    assert.string(b, 'bucket');
    assert.string(k, 'key');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var client = this.getClient(cb);
    if (client)
        objects.getObject(client, b, k, opts, cb);
};


/**
 * Deletes an Object
 *
 * See https://mo.joyent.com/docs/moray/master/#DeleteObject for more info.
 *
 * @param {String} b    - Bucket name
 * @param {String} k    - Key name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.deleteObject = function deleteObject(b, k, opts, cb) {
    assert.string(b, 'bucket');
    assert.string(k, 'key');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var client = this.getClient(cb);
    if (client)
        objects.deleteObject(client, b, k, opts, cb);
};
MorayClient.prototype.delObject = MorayClient.prototype.deleteObject;


/**
 * Finds object matching a given filter
 *
 * See https://mo.joyent.com/docs/moray/master/#FindObjects for more info.
 *
 * @param {String} b      - Bucket name
 * @param {String} f      - Object filter
 * @param {Object} opts   - request parameters
 * @return {EventEmitter} - listen for 'record', 'end' and 'error'
 */
MorayClient.prototype.findObjects = function findObjects(b, f, opts) {
    assert.string(b, 'bucket');
    assert.string(f, 'filter');
    assert.optionalObject(opts, 'options');

    var client = this.getClient();
    if (client)
        return (objects.findObjects(client, b, f, (opts || {})));

    return (null);
};
MorayClient.prototype.find = MorayClient.prototype.findObjects;


/**
 * Idempotently Creates or Replaces a set of Object.
 *
 * See https://mo.joyent.com/docs/moray/master/#Batch for more info.
 *
 * @param {Array} requests - {bucket, key, value} tuples
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.batch = function batch(requests, opts, cb) {
    assert.arrayOfObject(requests, 'requests');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var client = this.getClient(cb);
    if (client)
        objects.batch(client, requests, opts, cb);
};


/**
 * Updates a set of object attributes.
 *
 * See https://mo.joyent.com/docs/moray/master/#UpdateObjects for more info.
 *
 * @param {String} bucket - bucket
 * @param {Object} fields - attributes to update (must be indexes)
 * @param {String} filter - update objects matching this filter
 * @param {Object} opts   - request parameters
 * @param {Function} cb   - callback
 */
MorayClient.prototype.updateObjects = function update(b, f, f2, opts, cb) {
    assert.string(b, 'bucket');
    assert.object(f, 'fields');
    assert.string(f2, 'filter');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var client = this.getClient(cb);
    if (client)
        objects.updateObjects(client, b, f, f2, opts, cb);
};


/**
 * Deletes a group of objects.
 *
 * See https://mo.joyent.com/docs/moray/master/#DeleteMany for more info.
 *
 * @param {String} bucket - bucket
 * @param {String} filter - update objects matching this filter
 * @param {Object} opts   - request parameters
 * @param {Function} cb   - callback
 */
MorayClient.prototype.deleteMany = function deleteMany(b, f, opts, cb) {
    assert.string(b, 'bucket');
    assert.string(f, 'filter');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var client = this.getClient(cb);
    if (client)
        objects.deleteMany(client, b, f, opts, cb);
};


/*
 * Gets the set of tokens from moray.
 *
 * See https://mo.joyent.com/docs/moray/master/#UpdateObjects for more info.
 *
 * @param {Object} opts   - request parameters
 * @param {Function} cb   - callback
 */
MorayClient.prototype.getTokens = function getTokens(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var client = this.getClient(cb);
    if (client)
        tokens.getTokens(client, opts, cb);
};


/**
 * Performs a ping check against the server.
 *
 * Note that because the MorayClient is pooled and connects to all IPs in
 * a RR-DNS set, this actually just tells you that _one_ of the servers is
 * responding, not that all are.
 *
 * In most cases, you probably want to send in '{deep: true}' as options
 * so a DB-level check is performed on the server.
 *
 * @param {Object} opts   - request parameters
 * @return {EventEmitter} - listen for 'record', 'end' and 'error'
 */
MorayClient.prototype.ping = function _ping(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var client = this.getClient(cb);
    if (client)
        ping(client, opts, cb);
};


/**
 * Performs a raw SQL operation against the server.
 *
 * For the love of all this good in this earth, please only use this method
 * at the utmost of need.  In almost every case this is used, it's used for
 * nefarious reasons.
 *
 * The only intended uses of this are for edge cases that require custom
 * "serial" tables, et al, in the database (like UFDS changelog).  You
 * absolutely do not ever need to use this if put/get/del/find works for you.
 *
 * @param {String} stmt   - SQL Statement
 * @param {Array} vals    - Values (if SQL statement has $1 etc. in it)
 * @param {Object} opts   - Request Options
 * @return {EventEmitter} - listen for 'record', 'end' and 'error'
 */
MorayClient.prototype.sql = function _sql(stmt, vals, opts) {
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

    var client = this.getClient();
    if (client)
        return (sql(client, stmt, vals, opts));

    return (null);
};


MorayClient.prototype.toString = function toString() {
    var str = sprintf('[object MorayClient<host=%s>]', this.host);
    return (str);
};


// A little bit goofy, as this takes an optional callback which will
// automatically return from callers (this is really an internal)
// function - if the API takes no callback then the behavior is to
// emit an error
MorayClient.prototype.getClient = function getClient(callback) {
    assert.optionalFunc(callback, 'callback');

    var self = this;
    var cb = once(function _cb(err) {
        if (!callback) {
            self.emit('error', err);
        } else {
            callback(err);
        }
    });

    if (!this.pool) {
        cb(new Error('no connection pool'));
        return (null);
    }

    var c;
    var client;
    var pool = this.pool;
    var ring = pool.next();
    var subStart;
    var start = pool.iter;

    do {
        subStart = ring.iter;
        client = ring.next();
        do {
            c = client.conn;
            if (c && c.readable && c.writable && !c.destroyed)
                return (client);

        } while ((client = ring.next()) && ring.iter !== subStart);

    } while ((ring = pool.next()) && pool.iter !== start);

    if (!ring) {
        cb(new Error('connection pool corrupt'));
        return (null);
    }

    // If we're here, we'll just return a dead client and let the top
    // emit error
    return (client || ring.next());
};



///--- Exports

module.exports = {
    Client: MorayClient
};






// ///--- Tests

// (function runTest() {
//         var bunyan = require('bunyan');

//         var log = bunyan.createLogger({
//                 name: 'moray_test',
//                 stream: process.stdout,
//                 level: process.env.LOG_LEVEL || 'debug',
//                 serializers: bunyan.stdSerializers
//         });

//         var client = new MorayClient({
//                 checkInterval: 10 * 1000,
//                 connectTimeout: 4000,
//                 host: '1.moray.bh1.joyent.us',
//                 port: 2020,
//                 log: log,
//                 maxConnections: 10,
//                 maxIdleTime: 600 * 1000,
//                 pingTimeout: 4000
//         });

//         function onConnect() {
//                 client.once('close', function () {
//                         log.info('runTest: client closed');
//                 });

//                 client.once('error', function (err) {
//                         log.error(err, 'runTest: error encountered');
//                 });

//                 client.once('connect', function () {
//                         log.info('runTest: reconnected');
//                 });

//                 console.log('go stop haproxy in the moray zone...');
//         }

//         client.once('connect', onConnect);
// })();
