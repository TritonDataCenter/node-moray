// Copyright 2013 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var once = require('once');
var uuid = require('node-uuid');

var utils = require('./utils');



///--- Helpers

function copyOptions(options, value) {
        var opts = {
                etag: options.etag !== undefined ? options.etag : options._etag,
                headers: options.headers || {},
                limit: options.limit,
                noCache: true,
                offset: options.offset,
                req_id: options.req_id || uuid.v1(),
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

        req.once('message', function(obj) {
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
