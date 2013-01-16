// Copyright (c) 2013, Joyent, Inc. All rights reserved.

var assert = require('assert-plus');
var bunyan = require('bunyan');
var uuid = require('node-uuid');

var moray = require('../lib');



///--- Globals

var BUCKET = 't' + uuid.v4().substr(0, 7);
var CONFIG = {
        index: {
                foo: {
                        type: 'string'
                }
        }
};



///--- Mainline
(function main() {
        var client = moray.createClient({
                connectTimeout: 1000,
                dns: {
                        checkInterval: 2000,
                        // resolvers: ['10.99.99.201'],
                        timeout: 500
                },
                log: bunyan.createLogger({
                        name: 'moray_client',
                        level: process.env.LOG_LEVEL || 'debug',
                        stream: process.stdout
                }),
                max: 10,
                retry: {retries: 2},
                url: process.env.MORAY_URL || 'tcp://127.0.0.1:2020'
        });

        client.once('connect', function () {
                client.putBucket(BUCKET, CONFIG, function (init_err) {
                        assert.ifError(init_err);

                        function run() {
                                var done = 0;
                                function cb(err) {
                                        if (err)
                                                console.error('put failed: ' +
                                                              err.stack);

                                        if (++done === 10)
                                                setTimeout(run, 10000);
                                }

                                for (var i = 0; i < 10; i++) {
                                        var k = uuid.v1();
                                        var v = {
                                                foo: '' + i
                                        };
                                        client.putObject(BUCKET, k, v, cb);
                                }
                        }

                        run();
                });
        });
})();
