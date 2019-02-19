/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/buckets.js: bucket-related client API functions.  These functions are
 * invoked by same-named methods in lib/client.js to do the bulk of the work
 * associated with making RPC requests.  The arguments and semantics of these
 * functions are documented in the Moray API.
 */

var EventEmitter = require('events').EventEmitter;

var assert = require('assert-plus');
var libuuid = require('libuuid');
var jsprim = require('jsprim');
var VError = require('verror');

var rpc = require('./rpc');


///--- API

function createBucket(rpcctx, owner, bucket, vnode, callback) {
    var cfg, opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');

    // cfg = serializeBucketConfig(config);
    opts = makeBucketOptions({});

    var arg = { owner: owner,
                name: bucket,
                vnode: vnode
              };

    log = rpc.childLogger(rpcctx, opts);

    /*
     * electric-moray sends trailing null values with this response.  These are
     * not normally allowed unless we specify ignoreNullValues.
     */
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'putbucket',
        'rpcargs': [arg],
        'ignoreNullValues': true,
        'log': log
    }, callback);
}

function createBucketNoVnode(rpcctx, owner, bucket, callback) {
    var cfg, opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.func(callback, 'callback');

    // cfg = serializeBucketConfig(config);
    opts = makeBucketOptions({});

    log = rpc.childLogger(rpcctx, opts);

    /*
     * electric-moray sends trailing null values with this response.  These are
     * not normally allowed unless we specify ignoreNullValues.
     */
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'createbucket',
        'rpcargs': [owner, bucket],
        'ignoreNullValues': true,
        'log': log
    }, function (err, buckets) {
        if (!err && buckets.length != 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, buckets[0]);
        }
    });
}

function getBucket(rpcctx, owner, bucket, vnode, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');

    opts = makeBucketOptions({});

    var arg = { owner: owner,
                name: bucket,
                vnode: vnode
              };

    log = rpc.childLogger(rpcctx, opts);

    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'getbucket',
        'rpcargs': [arg],
        'log': log
    }, function (err, buckets) {
        if (!err && buckets.length != 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, buckets[0]);
        }
    });
}

function getBucketNoVnode(rpcctx, owner, bucket, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.func(callback, 'callback');

    opts = makeBucketOptions({});

    log = rpc.childLogger(rpcctx, opts);

    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'getbucket',
        'rpcargs': [owner, bucket],
        'log': log
    }, function (err, buckets) {
        if (!err && buckets.length != 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, buckets[0]);
        }
    });
}

function deleteBucket(rpcctx, owner, bucket, vnode, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');

    opts = makeBucketOptions({});

    var arg = { owner: owner,
                name: bucket,
                vnode: vnode
              };

    log = rpc.childLogger(rpcctx, opts);

    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'deletebucket',
        'rpcargs': [arg],
        'ignoreNullValues': true,
        'log': log
    }, function (err, buckets) {
        if (!err && buckets.length != 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, buckets[0]);
        }
    });
}

function deleteBucketNoVnode(rpcctx, owner, bucket, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.func(callback, 'callback');

    opts = makeBucketOptions({});

    log = rpc.childLogger(rpcctx, opts);

    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'deletebucket',
        'rpcargs': [owner, bucket],
        'ignoreNullValues': true,
        'log': log
    }, function (err, buckets) {
        if (err) {
            callback(err);
        } else {
            callback(null, buckets);
        }
    });
}

function listBuckets(rpcctx, owner, vnode) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.number(vnode, 'vnode');

    opts = makeBucketOptions({});

    var arg = {
        owner: owner,
        vnode: vnode
    };

    log = rpc.childLogger(rpcctx, opts);

    var res = new EventEmitter();

    rpc.rpcCommonBufferData({
        rpcctx: rpcctx,
        rpcmethod: 'listbuckets',
        rpcargs: [arg],
        log: log
    }, function (err, buckets) {
        if (err) {
            res.emit('error', err);
            return;
        }

        buckets.forEach(function (bucket) {
            res.emit('record', bucket);
        });

        res.emit('_moray_internal_rpc_done');
        res.emit('end');
    });

    return (res);
}

function listBucketsNoVnode(rpcctx, owner) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');

    opts = makeBucketOptions({});

    log = rpc.childLogger(rpcctx, opts);

    var res = new EventEmitter();

    rpc.rpcCommonBufferData({
        rpcctx: rpcctx,
        rpcmethod: 'listbuckets',
        rpcargs: [owner],
        log: log
    }, function (err, buckets) {
        if (err) {
            res.emit('error', err);
            return;
        }

        buckets.forEach(function (bucket) {
            res.emit('record', bucket);
        });

        res.emit('_moray_internal_rpc_done');
        res.emit('end');
    });

    return (res);
}


///--- Helpers

/*
 * Create a shallow copy of the given configuration, but serialize functions
 * in the "pre" and "post" arrays.
 */
function serializeBucketConfig(config) {
    var cfg, k;

    cfg = {};
    if (typeof (config) == 'object' && config !== null) {
        for (k in config) {
            cfg[k] = config[k];
        }
    }

    cfg.pre = (config.pre || []).map(function (f) {
        return (f.toString());
    });
    cfg.post = (config.post || []).map(function (f) {
        return (f.toString());
    });

    return (cfg);
}

/*
 * Create options suitable for a bucket-related RPC call by creating a deep copy
 * of the options passed in by the caller.  If the caller did not specify a
 * req_id, create one and add it to the returned options.
 */
function makeBucketOptions(options) {
    var opts = jsprim.deepCopy(options);
    opts.req_id = options.req_id || libuuid.create();
    return (opts);
}

/* XXX will a bad bucket here will crash the client? */
function parseBucketConfig(obj) {
    function parseFunctor(f) {
        var fn;
        /* jsl:ignore */
        eval('fn = ' + f);
        /* jsl:end */
        return (fn);
    }
    var res = {
        name: obj.name,
        index: JSON.parse(obj.index),
        pre: JSON.parse(obj.pre).map(parseFunctor),
        post: JSON.parse(obj.post).map(parseFunctor),
        options: JSON.parse(obj.options),
        mtime: new Date(obj.mtime)
    };
    if (obj.reindex_active) {
        res.reindex_active = JSON.parse(obj.reindex_active);
    }
    return (res);
}


///--- Exports

module.exports = {
    createBucket: createBucket,
    createBucketNoVnode: createBucketNoVnode,
    getBucket: getBucket,
    getBucketNoVnode: getBucketNoVnode,
    deleteBucket: deleteBucket,
    deleteBucketNoVnode: deleteBucketNoVnode,
    listBuckets: listBuckets,
    listBucketsNoVnode: listBucketsNoVnode
};
