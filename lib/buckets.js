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

var assert = require('assert-plus');
var libuuid = require('libuuid');
var jsprim = require('jsprim');
var VError = require('verror');

var rpc = require('./rpc');


///--- API

function createBucket(rpcctx, bucket, config, options, callback) {
    var cfg, opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.object(config, 'config');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    cfg = serializeBucketConfig(config);
    opts = makeBucketOptions(options);
    log = rpc.childLogger(rpcctx, opts);

    /*
     * electric-moray sends trailing null values with this response.  These are
     * not normally allowed unless we specify ignoreNullValues.
     */
    rpc.rpcCommonNoData({
        'rpcctx': rpcctx,
        'rpcmethod': 'createBucket',
        'rpcargs': [ bucket, cfg, opts ],
        'ignoreNullValues': true,
        'log': log
    }, callback);
}

function getBucket(rpcctx, bucket, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeBucketOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'getBucket',
        'rpcargs': [ opts, bucket ],
        'log': log
    }, function (err, buckets) {
        if (!err && buckets.length != 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, parseBucketConfig(buckets[0]));
        }
    });
}

function listBuckets(rpcctx, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeBucketOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'rpcmethod': 'listBuckets',
        'rpcargs': [ opts ],
        'log': log
    }, function (err, buckets) {
        if (err) {
            callback(err);
        } else {
            callback(null, buckets.map(parseBucketConfig));
        }
    });
}

function updateBucket(rpcctx, bucket, config, options, callback) {
    var cfg, opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.object(config, 'config');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    cfg = serializeBucketConfig(config);
    opts = makeBucketOptions(options);
    log = rpc.childLogger(rpcctx, opts);

    /*
     * electric-moray sends trailing null values with this response.  These are
     * not normally allowed unless we specify ignoreNullValues.
     */
    rpc.rpcCommonNoData({
        'rpcctx': rpcctx,
        'rpcmethod': 'updateBucket',
        'rpcargs': [ bucket, cfg, opts ],
        'ignoreNullValues': true,
        'log': log
    }, callback);
}

function deleteBucket(rpcctx, bucket, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeBucketOptions(options);
    opts.bucket = bucket;
    log = rpc.childLogger(rpcctx, opts);

    /*
     * electric-moray sends trailing null values with this response.  These are
     * not normally allowed unless we specify ignoreNullValues.
     */
    rpc.rpcCommonNoData({
        'rpcctx': rpcctx,
        'rpcmethod': 'delBucket',
        'rpcargs': [ bucket, opts ],
        'ignoreNullValues': true,
        'log': log
    }, callback);
}

function putBucket(rpcctx, b, cfg, options, cb) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(b, 'bucket');
    assert.object(cfg, 'config');
    assert.object(options, 'options');
    assert.func(cb, 'callback');

    var opts = makeBucketOptions(options);

    function putCallback(err) {
        // MANTA-1342: multiple racers doing putBucket
        // get this back b/c there's no way to be idempotent
        // with tables in postgres.  So we just check for that
        // error code and eat it -- this is somewhat dangerous
        // if two callers weren't doing the same putBucket, but
        // that's not really ever what we see in practice.
        if (err &&
            VError.findCauseByName(err, 'BucketConflictError') === null) {
            cb(err);
        } else {
            cb();
        }
    }

    getBucket(rpcctx, b, opts, function (err, bucket) {
        if (err) {
            if (VError.findCauseByName(err, 'BucketNotFoundError') !== null) {
                createBucket(rpcctx, b, cfg, opts, putCallback);
            } else {
                putCallback(err);
            }
        } else {
            // MANTA-897 - short circuit client side if
            // versions are equivalent
            var v = bucket.options.version;
            var v2 = (cfg.options || {}).version || 0;
            if (v !== 0 && v === v2) {
                putCallback();
            } else {
                updateBucket(rpcctx, b, cfg, opts, putCallback);
            }
        }
    });
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
    getBucket: getBucket,
    listBuckets: listBuckets,
    updateBucket: updateBucket,
    deleteBucket: deleteBucket,
    putBucket: putBucket
};
