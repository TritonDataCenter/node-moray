/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/rpc.js: RPC-related utility functions
 */

var assert = require('assert-plus');
var VError = require('verror');


///--- API

function childLogger(rpcctx, options) {
    return (rpcctx.createLog(options));
}

/*
 * We provide a few helper methods for making RPC calls using a FastClient:
 *
 *     rpcCommonNoData(args, callback)
 *
 *           Makes an RPC using the arguments specified by "args", reads and
 *           buffers data returned by the RPC, waits for the RPC to complete,
 *           and then invokes "callback" as:
 *
 *               callback(err)
 *
 *           where "err" is an error if the RPC fails or if it succeeds but
 *           returns any data (which is not expected for users of this
 *           function).
 *
 *     rpcCommonBufferData(args, callback)
 *
 *           Makes an RPC using the arguments specified by "args", reads and
 *           buffers data returned by the RPC, waits for the RPC to complete,
 *           and then invokes "callback" as:
 *
 *               callback(err, data)
 *
 *           where "err" is an error only if the RPC fails.  If the RPC
 *           succeeds, "data" is an array of data emitted by the RPC call.
 *
 *     rpcCommon(args, callback)
 *
 *           Makes an RPC using the arguments specified by "args", waits for the
 *           RPC to complete, and then invokes "callback" as:
 *
 *               callback(err)
 *
 *           where "err" is an error only if the RPC fails.  Unlike the
 *           interfaces above, this one does NOT read data from the RPC, which
 *           means the caller is responsible for doing that.  Since the RPC
 *           response is a stream, if the caller does not start reading data,
 *           the RPC will never complete.
 *
 *           This is the building block for the other methods, but it's only
 *           useful if you want to handle data emitted by the RPC in a streaming
 *           way.
 *
 * All of these functions take care of unwrapping errors if the Moray client was
 * configured that way.  Named arguments are:
 *
 *     rpcmethod, rpcargs, log,     See FastClient.rpc() method.
 *     ignoreNullValues
 *
 *     rpcctx                       Moray's "rpcctx" handle, a wrapper around
 *                                  a FastClient that includes context related
 *                                  to this Moray client.
 */
function rpcCommon(args, callback) {
    var rpcctx, req, addrs;

    assert.object(args, 'args');
    assert.object(args.rpcctx, 'args.rpcctx');
    assert.string(args.rpcmethod, 'args.rpcmethod');
    assert.array(args.rpcargs, 'args.rpcargs');
    assert.object(args.log, 'args.log');
    assert.optionalNumber(args.timeout, 'args.timeout');
    assert.optionalBool(args.ignoreNullValues, 'args.ignoreNullValues');
    assert.func(callback);

    rpcctx = args.rpcctx;
    req = rpcctx.fastClient().rpc({
        'rpcmethod': args.rpcmethod,
        'rpcargs': args.rpcargs,
        'timeout': args.timeout,
        'ignoreNullValues': args.ignoreNullValues,
        'log': args.log
    });

    req.once('end', callback);
    req.once('error', function (err) {
        if (rpcctx.unwrapErrors()) {
            err = unwrapError(err);
        } else {
            addrs = rpcctx.socketAddrs();
            err = new VError({
                'cause': err,
                'info': addrs
            }, 'moray client ("%s" to "%s")', addrs.local, addrs.remote);
        }
        callback(err);
    });

    return (req);
}

/*
 * See above.
 */
function rpcCommonNoData(args, callback) {
    assert.func(callback);
    rpcCommonBufferData(args, function (err, data) {
        if (!err && data.length > 0) {
            err = new VError('bad server response: expected 0 data messages, ' +
                'found %d\n', data.length);
        }

        callback(err);
    });
}

/*
 * See above.
 */
function rpcCommonBufferData(args, callback) {
    var req, rpcmethod, log, data;

    assert.func(callback, 'callback');
    req = rpcCommon(args, function (err) {
        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
    });

    rpcmethod = args.rpcmethod;
    log = args.log;
    data = [];
    req.on('data', function (obj) {
        log.debug({ 'message': obj }, 'rpc ' + rpcmethod + ': received object');
        data.push(obj);
    });

    return (req);
}

/*
 * See the "unwrapErrors" constructor argument for the MorayClient.
 */
function unwrapError(err) {
    if (err.name == 'FastRequestError') {
        err = VError.cause(err);
    }

    if (err.name == 'FastServerError') {
        err = VError.cause(err);
    }

    return (err);
}


///--- Exports

module.exports = {
    childLogger: childLogger,
    rpcCommon: rpcCommon,
    rpcCommonNoData: rpcCommonNoData,
    rpcCommonBufferData: rpcCommonBufferData
};
