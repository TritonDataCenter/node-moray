// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var net = require('net');

var assert = require('assert-plus');
var backoff = require('backoff');
var fast = require('fast');
var once = require('once');

var dns = require('./dns');
var Ring = require('./ring');



///--- Internals

function logAttempt(log, host) {
    function _log(number, delay) {
        var level;
        if (number === 0) {
            level = 'info';
        } else if (number < 5) {
            level = 'warn';
        } else {
            level = 'error';
        }
        log[level]({
            ip: host,
            attempt: number,
            delay: delay
        }, 'connect attempted');
    }

    return (_log);
}


function inDNS(opts, cb) {
    dns.resolve(opts.dns_opts, function (err, ips) {
        if (err) {
            cb(err);
        } else if (!(ips || []).some(function (ip) {
            return (ip === opts.host);
        })) {
            cb(new Error('Host no longer in DNS'));
        } else {
            cb();
        }
    });
}


function connect(opts, callback) {
    assert.object(opts, 'options');
    assert.object(opts.dns_opts, 'options.dns_opts');
    assert.func(callback, 'callback');

    callback = once(callback);
    var log = opts.log;

    function _connect(_, cb) {
        cb = once(cb);

        function __connect() {
            var client = fast.createClient(opts);

            client.on('connectAttempt', logAttempt(log, opts.host));
            client.on('connectAttempt', function () {
                inDNS(opts, function (err) {
                    if (err) {
                        log.error(err, '%s not in DNS (%s), abandoning',
                                  opts.host, opts.dns_opts.domain);
                        client.close();
                    }
                });
            });

            client.once('connect', function onConnect() {
                client.removeAllListeners('error');
                cb(null, client);
            });

            client.once('error', function onConnectError(err) {
                client.removeAllListeners('connect');
                cb(err);
            });
        }

        if (net.isIP(opts.dns_opts.domain)) {
            __connect();
        } else {
            inDNS(opts, function (err) {
                if (err) {
                    log.error(err, '%s not in DNS (%s)',
                              opts.host, opts.dns_opts.domain);
                    retry.failAfter(1);
                    cb(err);
                } else {
                    __connect();
                }
            });
        }
    }

    var retry = backoff.call(_connect, {}, function (err, client) {
        retry.removeAllListeners('backoff');
        if (!err) {
            log.debug('moray: connected to %s after %d attempts',
                      opts.host, retry.getResults().length);
        }
        callback(err, client);
    });

    retry.setStrategy(new backoff.ExponentialStrategy({
        initialDelay: opts.minTimeout || 100,
        maxDelay: opts.maxTimeout || 60000
    }));
    retry.failAfter(opts.retries || Infinity);
    retry.on('backoff', logAttempt(log));

    retry.start();
}


function createConnectionRing(opts, cb) {
    assert.object(opts, 'options');
    assert.string(opts.host, 'options.host');
    assert.object(opts.log, 'options.log');
    assert.number(opts.max, 'options.max');
    assert.func(cb, 'callback');

    cb = once(cb);

    var done = 0;
    var log = opts.log.child({
        host: opts.host
    }, true);
    var ring = new Ring(opts.max);
    ring.close = function close() {
        var dead = 0;
        var total = ring.length;

        function onClose(conn) {
            conn.removeAllListeners('close');
            conn.removeAllListeners('connect');
            conn.removeAllListeners('error');
            if (++dead === total)
                ring.emit('close');
        }

        for (var j = 0; j < total; j++) {
            var c = ring.pop();
            c._deadbeef = true;
            c.once('close', onClose.bind(this, c));
            c.close();
        }
    };

    function connect_cb(connect_err, client) {
        if (connect_err) {
            log.error(connect_err, 'unable to connect client');
            cb(connect_err);
            return;
        }

        client.log = log;

        // Note that node-fast has reconnect logic, so we just want to
        // capture that events happened, and let it redrive
        client.on('error', function (err) {
            if (!client._deadbeef)
                log.error(err, 'client error');
        });

        client.on('close', function () {
            if (!client._deadbeef)
                log.warn('connection closed');
        });

        client.on('connect', function () {
            if (!client._deadbeef)
                log.info('reconnected');
        });

        ring.push(client);
        if (++done === opts.max)
            cb(null, ring);
    }

    for (var i = 0; i < opts.max; i++)
        connect(opts, connect_cb);
}



///--- API

function createConnectionPool(options, callback) {
    assert.object(options, 'options');
    assert.number(options.connectTimeout, 'options.connectTimeout');
    assert.object(options.dns, 'options.dns');
    assert.string(options.host, 'options.host');
    assert.object(options.log, 'options.log');
    assert.number(options.max, 'options.max');
    assert.number(options.port, 'options.port');
    assert.optionalObject(options.retry, 'options.retry');
    assert.func(callback, 'callback');

    callback = once(callback);

    var dns_opts = {
        domain: options.host,
        log: options.log,
        resolvers: options.dns.resolvers,
        timeout: options.dns.timeout || 3000
    };

    function create(err, ips) {
        if (err) {
            callback(err);
            return;
        }

        var done = 0;
        var ndx = 0;
        var ring = new Ring(ips.length);

        ring.close = function close() {
            var copy = ring.ring.slice(0);
            var dead = 0;

            function onClose() {
                if (++dead === copy.length)
                    ring.emit('close');
            }

            copy.forEach(function (c) {
                c.once('close', onClose);
                c.close();
                ring.remove(c);
            });
        };
        ring.table = {};

        ips.forEach(function (ip) {
            ring.table[ip] = ndx++;
            var opts = {
                connectTimeout: options.connectTimeout,
                dns_opts: dns_opts,
                host: ip,
                log: options.log,
                max: options.max,
                port: options.port,
                retry: options.retry
            };
            createConnectionRing(opts, function (ring_err, r) {
                if (ring_err) {
                    ring.close();
                    callback(ring_err);
                    return;
                }
                ring.push(r);

                if (++done === ips.length)
                    callback(null, ring);
            });
        });
    }

    if (net.isIP(options.host)) {
        create(null, [options.host]);
    } else {
        dns.resolve(dns_opts, create);
    }
}



///--- Exports

module.exports = {
    createConnectionPool: createConnectionPool
};
