/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

var assert = require('assert-plus');
var libuuid = require('libuuid');
var once = require('once');

var utils = require('./utils');



///--- Helpers

function copyOptions(options, value) {
    var opts = {
        etag: options.etag !== undefined ? options.etag : options._etag,
        headers: options.headers || {},
        limit: options.limit,
        noCache: true,
        offset: options.offset,
        req_id: options.req_id || libuuid.create(),
        sort: options.sort,
        vnode: options.vnode
    };

    if (value)
        opts._value = JSON.stringify(value);
    if (typeof (options.noCache) !== 'undefined')
        opts.noCache = options.noCache;

    return (opts);
}



///--- API
// All of the functions here are scoped to the file as they require an
// underlying 'fast' connection to be given to them.  The functions on
// the moray prototype are just passthroughs to all these

function getTokens(client, options, callback) {
    assert.object(client, 'client');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    callback = once(callback);

    var req;
    var res;
    var log = utils.childLogger(client);
    var opts = copyOptions(options);

    req = client.rpc('getTokens', opts);

    log.debug('getTokens: entered');

    req.once('message', function (obj) {
        res = {
            tokens: obj
        };
        log.debug({
            tokens: obj
        }, 'getTokens: got tokens');
    });

    req.once('end', function () {
        log.debug('getTokens: done');
        callback(null, res);
    });

    req.once('error', function (err) {
        log.debug(err, 'getTokens: failed');
        callback(err);
    });
}



///--- Exports

module.exports = {
    getTokens: getTokens
};
