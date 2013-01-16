// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var clone = require('clone');
var once = require('once');
var uuid = require('node-uuid');

var utils = require('./utils');



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
                req_id: options.req_id || uuid.v1()
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


function putBucket(client, bucket, config, options, callback) {
        assert.object(client, 'client');
        assert.string(bucket, 'bucket');
        assert.object(config, 'config');
        assert.object(options, 'options');
        assert.func(callback, 'callback');

        var opts = copyOptions(options);

        getBucket(client, bucket, opts, function (err) {
                if (err) {
                        // TODO if (err.name === 'BucketNotFoundError')
                        createBucket(client, bucket, config, opts, callback);
                } else {
                        updateBucket(client, bucket, config, opts, callback);
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