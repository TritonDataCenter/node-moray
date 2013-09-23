// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var libuuid = require('libuuid');
var once = require('once');

var utils = require('./utils');



///--- Globals

var clone = utils.clone;


///--- Helpers

function copyConfig(config) {
    var cfg = clone(config);

    cfg.pre = (config.pre || []).map(function (f) {
        return (f.toString());
    });
    cfg.post = (config.post || []).map(function (f) {
        return (f.toString());
    });

    return (cfg);
}


function copyOptions(options) {
    var opts = {
        req_id: options.req_id || libuuid.create(),
        timeout: options.timeout
    };

    return (opts);
}



///--- API
// All of the functions here are scoped to the file as they require an
// underlying 'fast' connection to be given to them.  The functions on
// the moray prototype are just passthroughs to all these

function createBucket(client, bucket, config, options, callback) {
    assert.object(client, 'client');
    assert.string(bucket, 'bucket');
    assert.object(config, 'config');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    callback = once(callback);

    var cfg = copyConfig(config);
    var opts = copyOptions(options);
    var req = client.rpc('createBucket', bucket, cfg, opts);
    var log = utils.childLogger(client, opts);

    log.debug({
        bucket: bucket,
        config: cfg
    }, 'createBucket: entered');

    var cb = utils.simpleCallback({
        callback: callback,
        log: log,
        name: 'createBucket',
        request: req
    });

    req.once('end', cb);
    req.once('error', cb);
}


function getBucket(client, bucket, options, callback) {
    assert.object(client, 'client');
    assert.string(bucket, 'bucket');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    callback = once(callback);

    var opts = copyOptions(options);
    var req;
    var res;
    var log = utils.childLogger(client, opts);

    log.debug({
        bucket: bucket
    }, 'getBucket: entered');

    req = client.rpc('getBucket', opts, bucket);

    req.once('message', function (obj) {
        function parseFunctor(f) {
            var fn;
            /* jsl:ignore */
            eval('fn = ' + f);
            /* jsl:end */
            return (fn);
        }
        res = {
            name: obj.name,
            index: JSON.parse(obj.index),
            pre: JSON.parse(obj.pre).map(parseFunctor),
            post: JSON.parse(obj.post).map(parseFunctor),
            options: JSON.parse(obj.options),
            mtime: new Date(obj.mtime)
        };
        log.debug({
            message: obj
        }, 'getBucket: bucket found');
    });

    req.once('end', function () {
        log.debug('getBucket: done');
        callback(null, res);
    });

    req.once('error', function (err) {
        log.debug(err, 'getBucket: failed');
        callback(err);
    });
}


function updateBucket(client, bucket, config, options, callback) {
    assert.object(client, 'client');
    assert.string(bucket, 'bucket');
    assert.object(config, 'config');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    callback = once(callback);

    var cfg = copyConfig(config);
    var opts = copyOptions(options);
    var req = client.rpc('updateBucket', bucket, cfg, opts);
    var log = utils.childLogger(client, opts);

    log.debug({
        bucket: bucket,
        config: cfg
    }, 'updateBucket: entered');

    var cb = utils.simpleCallback({
        callback: callback,
        log: log,
        name: 'updateBucket',
        request: req
    });

    req.once('end', cb);
    req.once('error', cb);
}


function deleteBucket(client, bucket, options, callback) {
    assert.object(client, 'client');
    assert.string(bucket, 'bucket');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    callback = once(callback);

    var opts = copyOptions(options);
    var req;
    var log = utils.childLogger(client, opts);

    log.debug({
        bucket: bucket
    }, 'delBucket: entered');

    req = client.rpc('delBucket', bucket, options);

    req.once('end', function () {
        log.debug('delBucket: done');
        callback();
    });

    req.once('error', function (err) {
        log.debug(err, 'delBucket: failed');
        callback(err);
    });
}


function putBucket(client, b, cfg, options, cb) {
    assert.object(client, 'client');
    assert.string(b, 'bucket');
    assert.object(cfg, 'config');
    assert.object(options, 'options');
    assert.func(cb, 'callback');

    var opts = copyOptions(options);

    var _cb = once(function put_callback(err) {
        // MANTA-1342: multiple racers doing putBucket
        // get this back b/c there's no way to be idempotent
        // with tables in postgres.  So we just check for that
        // error code and eat it -- this is somewhat dangerous
        // if two callers weren't doing the same putBucket, but
        // that's not really ever what we see in practice.
        if (err && err.name !== 'BucketConflictError') {
            cb(err);
        } else {
            cb();
        }
    });

    getBucket(client, b, opts, function (err, bucket) {
        if (err) {
            if (err.name === 'BucketNotFoundError') {
                createBucket(client, b, cfg, opts, _cb);
            } else {
                _cb(err);
            }
        } else {
            // MANTA-897 - short circuit client side if
            // versions are equivalent
            var v = bucket.options.version;
            var v2 = (cfg.options || {}).version || 0;
            if (v !== 0 && v === v2) {
                _cb();
            } else {
                updateBucket(client, b, cfg, opts, _cb);
            }
        }
    });
}



///--- Exports

module.exports = {
    createBucket: createBucket,
    getBucket: getBucket,
    updateBucket: updateBucket,
    deleteBucket: deleteBucket,
    putBucket: putBucket
};
