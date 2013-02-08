// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var net = require('net');

var assert = require('assert-plus');
var backoff = require('backoff');
var fast = require('fast');
var once = require('once');

var dns = require('./dns');
var Ring = require('./ring');



///--- Internals

function connect(opts, callback) {
        assert.object(opts, 'options');
        assert.func(callback, 'callback');

        callback = once(callback);
        var log = opts.log;

        function _connect(_, cb) {
                var client = fast.createClient(opts);
                client.once('connect', function onConnect() {
                        client.removeAllListeners('error');
                        cb(null, client);
                });
                client.once('error', function onConnectError(err) {
                        client.removeAllListeners('connect');
                        cb(err);
                });
        }

        var retry = backoff.call(_connect, {}, function (err, client) {
                retry.removeAllListeners('backoff');
                log.debug('moray: connected to %s after %d attempts',
                          opts.host, retry.getResults().length);
                callback(err, client);
        });

        retry.setStrategy(new backoff.ExponentialStrategy({
                initialDelay: opts.minTimeout || 100,
                maxDelay: opts.maxTimeout || 60000
        }));
        retry.failAfter(opts.retries || Infinity);
        retry.on('backoff', function onBackoff(number, delay) {
                var level;
                if (number === 0) {
                        level = 'info';
                } else if (number < 5) {
                        level = 'warn';
                } else {
                        level = 'error';
                }
                log[level]({
                        attempt: number,
                        delay: delay
                }, 'connect attempted');
        });

        retry.start();
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
                timeout: options.dns.timeout || 1000
        };
        var log = options.log;

        function create(err, ips) {
                if (err) {
                        callback(err);
                        return;
                }

                var done = 0;
                var ring = new Ring(ips.length);
                var total = ips.length * options.max;

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
                var ndx = 0;

                ips.forEach(function (ip) {
                        ring.table[ip] = ndx++;

                        var opts = {
                                connectTimeout: options.connectTimeout,
                                host: ip,
                                log: options.log,
                                port: options.port,
                                retry: options.retry
                        };
                        var r = new Ring(options.max);

                        r.close = function _close() {
                                var dead = 0;
                                var total = r.length;

                                function onClose() {
                                        if (++dead === total)
                                                r.emit('close');
                                }

                                for (var j = 0; j < total; j++) {
                                        var c = r.pop();
                                        c.removeAllListeners('close');
                                        c.removeAllListeners('error');
                                        c.once('close', onClose);
                                        c.close();
                                }
                        };
                        ring.push(r);

                        for (var i = 0; i < options.max; i++) {
                                connect(opts, function _cb(err2, client) {
                                        if (err2) {
                                                callback(err2);
                                                return;
                                        }

                                        // Note that node-fast has reconnect
                                        // logic, so we just want to capture
                                        // that events happened, and let it
                                        // redrive
                                        client.on('error', function (err3) {
                                                log.error(err3, 'client error');
                                        });
                                        client.on('close', function () {
                                                log.warn('connection closed');
                                        });
                                        client.on('connect', function () {
                                                log.info('reconnected');
                                        });

                                        client.log = log.child({
                                                host: ip
                                        }, true);
                                        r.push(client);
                                        if (++done === total)
                                                callback(null, ring);
                                });
                        }
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



///--- Tests

// var test_opts = {
//         connectTimeout: 1000,
//         dns: {
//                 resolvers: ['10.99.99.201'],
//                 timeout: 500
//         },
//         host: '1.moray.laptop.joyent.us',
//         log: require('bunyan').createLogger({
//                 name: 'test',
//                 level: 'debug'
//         }),
//         max: 10,
//         port: 2020,
//         retry: {retries: 2}
// };

// createConnectionPool(test_opts, function (err, pool) {
//         assert.ifError(err);

//         console.log(pool.next().next().toString());
//         pool.close();
// });
