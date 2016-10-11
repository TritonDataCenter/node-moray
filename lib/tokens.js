/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/tokens.js: token-related client API functions.  These functions are
 * invoked by same-named methods in lib/client.js to do the bulk of the work
 * associated with making RPC requests.  The arguments and semantics of these
 * functions are documented in the Moray API.
 */

var assert = require('assert-plus');
var libuuid = require('libuuid');
var VError = require('verror');

var rpc = require('./rpc');


///--- API

function getTokens(rpcctx, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = { req_id: options.req_id || libuuid.create() };
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'getTokens',
        'rpcargs': [ opts ],
        'log': log
    }, function (err, tokens) {
        if (!err && tokens.length != 1) {
            err = new VError('bad server response: expected 1 token, found %d',
                tokens.length);
        }

        if (err) {
            callback(err);
            return;
        }

        callback(null, { 'tokens': tokens[0] });
    });
}


///--- Exports

module.exports = {
    getTokens: getTokens
};
