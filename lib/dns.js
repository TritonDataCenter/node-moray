// Copyright (c) 2012, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var backoff = require('backoff');
var dns = require('native-dns');
var once = require('once');



///--- API

function resolve(opts, callback) {
        assert.object(opts, 'options');
        assert.string(opts.domain, 'options.domain');
        assert.object(opts.log, 'options.log');
        assert.optionalArrayOfString(opts.resolvers, 'options.resolvers');
        assert.optionalNumber(opts.timeout, 'options.timeout');
        assert.func(callback, 'callback');

        callback = once(callback);

        var log = opts.log.child({domain: opts.domain}, true);
        var resolvers;

        if (opts.resolvers) {
                resolvers = opts.resolvers.map(function (r) {
                        return ({
                                address: r,
                                port: 53,
                                type: 'udp'
                        });
                });
        } else {
                resolvers = dns.platform.name_servers;
        }

        var _r = -1;
        resolvers.next = function () {
                if (++_r >= resolvers.length)
                        _r = 0;

                return (resolvers[_r]);
        };

        function _resolve(_, cb) {
                cb = once(cb);

                var answers = [];

                var req = dns.Request({
                        question: dns.Question({
                                name: opts.domain,
                                type: 'A'
                        }),
                        server: resolvers.next(),
                        timeout: opts.timeout || 1000,
                        cache: false
                });

                req.on('end', function onDNSDone() {
                        if (answers.length === 0) {
                                cb(new Error(opts.domain + ' not in DNS'));
                                return;
                        }
                        cb(null, answers);
                });
                req.on('message', function onDNSMessage(err, answer) {
                        if (err) {
                                cb(err);
                                return;
                        }
                        answer.answer.forEach(function (a) {
                                answers.push(a.address);
                        });
                });
                req.once('error', cb);
                req.once('timeout', function () {
                        cb(new Error('DNS Timeout'));
                });

                req.send();
        }


        function run() {
                var retry = backoff.call(_resolve, {}, function (err, answers) {
                        retry.removeAllListeners('backoff');
                        log.debug('resolve %s done after %d attempts',
                                  opts.domain, retry.getResults().length);
                        callback(err, answers);
                });
                retry.setStrategy(new backoff.ExponentialStrategy({
                        initialDelay: 100,
                        maxDelay: 30000
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
                        }, 'dns.resolve attempted');
                });
        }

        if (resolvers.length > 0) {
                run();
        } else {
                dns.platform.once('ready', run);
        }
}



///--- Exports

module.exports = {
        resolve: resolve
};
