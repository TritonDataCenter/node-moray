#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert');
var bunyan = require('bunyan');
var libuuid = require('libuuid');
var moray = require('./lib');

var client = moray.createClient({
    dns: {
        resolvers: ['10.99.99.11']
    },
    host: 'moray.coal.joyent.us',
    log: bunyan.createLogger({
        name: 'moray',
        level: process.env.LOG_LEVEL || 'fatal',
        stream: process.stdout,
        serializers: bunyan.stdSerializers
    }),
    port: 2020
});

client.once('connect', function () {
    client.getBucket('ufds_o_smartdc', function (err, cfg) {
        assert.ifError(err);

        console.log('starting...');

        var errors = 0;
        var map = {};
        var success = 0;
        var total = 0;
        (function run() {
            var id = libuuid.create();

            function next(_err) {
                delete map[id];

                if (_err) {
                    assert.ok(_err.name === 'ConnectionClosedError' ||
                              _err.name === 'NoConnectionError');
                    errors++;
                } else {
                    success++;
                }

                if (++total < 10000) {
                    if (total % 1000 === 0)
                        console.log(total);

                    setImmediate(run);
                } else {
                    assert.ok((success + errors) === total);
                    assert.ok(Object.keys(map).length === 0);
                    console.log('done');
                    client.close();
                }
            }

            var res = client.find('ufds_o_smartdc', 'login=*');

            map[id] = true;
            res.once('error', next);
            res.once('end', next);
            res.on('record', function (r) {
                assert.ok(r);
            });
        })();
    });
});
