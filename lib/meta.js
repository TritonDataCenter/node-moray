/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/meta.js: non-data-related client API functions.  These functions are
 * invoked by same-named methods in lib/client.js to do the bulk of the work
 * associated with making RPC requests.  The arguments and semantics of these
 * functions are documented in the Moray API.
 */

var assert = require('assert-plus');
var libuuid = require('libuuid');
var jsprim = require('jsprim');
var events = require('events');
var VError = require('verror');

var rpc = require('./rpc');


///--- API

function ping(rpcctx, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.object(options, 'options');
    assert.optionalNumber(options.timeout, 'options.timeout');
    assert.func(callback, 'callback');

    opts = {
        deep: options.deep || false,
        req_id: options.req_id || libuuid.create()
    };
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonNoData({
        'rpcctx': rpcctx,
        'rpcmethod': 'ping',
        'rpcargs': [ opts ],
        'log': log,
        'timeout': options.hasOwnProperty('timeout') ? options.timeout : 1000
    }, callback);
}

/*
 * This function invokes the "version" RPC on the remote server.  This should
 * not be used for any purpose except reporting the version to a human.
 *
 * It's tempting to use the reported version to determine whether the remote
 * server has some capabilities you need, but you MUST NOT do this.  First of
 * all, this client is backed by a pool of sockets, and the servers at the
 * remote ends of those sockets may be at different versions.  You could get
 * back "version 2" from this RPC, make another request that assumes version 2,
 * and have that fail because it contacted a version 1 server.  Note that this
 * is true even if this client is only connected to version 2 servers.
 * Operators may rollback Moray instances to earlier versions, so even in a
 * single-Moray deployment, you cannot assume that just because you got back
 * version N from this RPC that a subsequent request will not be serviced by a
 * version M server where M < N.
 *
 * By this point, it should be clear that the result of this RPC cannot be used
 * to make programmatic decisions.  It would be tempting to remove it
 * altogether so that people don't accidentally build dependencies on it (which
 * has happened in the past).  Instead, we give it a different name so that
 * people won't stumble upon it accidentally, but we can still build
 * command-line tools that report it.
 *
 * You might reasonably wonder: what do I do if I *do* depend on a newer Moray
 * version?  In that case, we'll need to rework the MorayClient abstraction so
 * that you can specify that all requests for this client should be made to
 * version-N servers.  The client will have to determine server version and keep
 * track of it.  None of this work has been done yet.  The minimum server
 * version supported by this client is 2, which is the current version.
 */
function versionInternal(rpcctx, options, callback) {
    var timeout, opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    /*
     * As mentioned above, servers that do not support this RPC will never
     * respond to it.  To provide behavior that's at least remotely sane, we
     * apply a generous timeout for this RPC.  However, in the event of timeout,
     * we will not conclude that the remote server is old.  We'll leave that for
     * callers to deal with.  See the notes above for details.
     */
    timeout = typeof (options.timeout) == 'number' ?
        options.timeout : 20000;
    opts = { req_id: options.req_id || libuuid.create() };
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'version',
        'rpcargs': [ opts ],
        'timeout': timeout,
        'log': log
    }, function (err, versions) {
        if (err) {
            /*
             * As described in detail above, there's a decent chance that a
             * timeout actually means the remote server is old.  This is
             * decidedly not obvious, so it's useful if we can augment the error
             * with a note about this possibility -- without actually concluding
             * that that's what happened, since we don't really know.
             */
            if (VError.findCauseByName(err, 'TimeoutError')) {
                err.message += ' (note: very old Moray versions do not ' +
                    'respond to this RPC)';
            }
        } else {
            if (versions.length != 1) {
                err = new VError('bad server response: expected 1 version, ' +
                    'but found %d', versions.length);
            } else if ((typeof (versions[0]) != 'object' ||
                versions[0] === null ||
                typeof (versions[0].version) != 'number')) {
                err = new VError(
                    'bad server response: unable to parse version');
            }
        }

        if (err) {
            callback(err);
        } else {
            callback(null, versions[0].version);
        }
    });
}

function sql(rpcctx, statement, values, options) {
    var opts, log, req, res;

    assert.object(rpcctx, 'rpcctx');
    assert.string(statement, 'statement');
    assert.ok(Array.isArray(values));
    assert.object(options, 'options');

    opts = { req_id: options.req_id || libuuid.create() };
    log = rpc.childLogger(rpcctx, opts);
    res = new events.EventEmitter();

    /*
     * We specify ignoreNullValues because electric-moray sends spurious
     * trailing null values from successful sql() commands.  These are not
     * generally allowed, but we have to maintain compatibility with broken
     * servers.
     */
    req = rpc.rpcCommon({
        'rpcctx': rpcctx,
        'rpcmethod': 'sql',
        'rpcargs': [ statement, values, opts ],
        'ignoreNullValues': true,
        'log': log
    }, function (err) {
        if (err) {
            res.emit('error', err);
        } else {
            res.emit('end');
        }
    });

    req.on('data', function (msg) {
        if (msg !== null) {
            log.debug('sql: msg: %j', msg);
            res.emit('record', msg);
        }
    });

    return (res);
}


///--- Exports

module.exports = {
    ping: ping,
    sql: sql,
    versionInternal: versionInternal
};
