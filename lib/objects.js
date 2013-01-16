// Copyright 2012 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;

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


function copyOptions(options, value) {
        var opts = {
                etag: options.etag || options._etag,
                headers: options.headers || {},
                limit: options.limit,
                noCache: true,
                offset: options.offset,
                req_id: options.req_id || uuid.v1(),
                sort: options.sort
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

function putObject(client, bucket, key, value, options, callback) {
        assert.object(client, 'client');
        assert.string(bucket, 'bucket');
        assert.string(key, 'key');
        assert.object(value, 'value');
        assert.object(options, 'options');
        assert.func(callback, 'callback');

        callback = once(callback);

        var opts = copyOptions(options, value);
        var log = utils.childLogger(client, opts);
        var meta;
        var req = client.rpc('putObject', bucket, key, value, opts);

        log.debug({
                bucket: bucket,
                key: key,
                value: value,
                etag: opts.etag
        }, 'putObject: entered');

        req.once('message', function (msg) {
                meta = msg;
        });

        req.once('end', function () {
                log.debug('putObject: done');
                callback(null, meta || {});
        });

        req.once('error', function (err) {
                log.debug(err, 'putObject: failed');
                callback(err);
        });
}


function getObject(client, bucket, key, options, callback) {
        assert.object(client, 'client');
        assert.string(bucket, 'bucket');
        assert.string(key, 'key');
        assert.object(options, 'options');
        assert.func(callback, 'callback');

        callback = once(callback);

        var opts = copyOptions(options);
        var log = utils.childLogger(client, opts);
        var obj;
        var req = client.rpc('getObject', bucket, key, opts);

        log.debug({
                bucket: bucket,
                key: key
        }, 'getObject: entered');

        req.once('message', function (msg) {
                obj = msg;
        });

        req.once('end', function () {
                log.debug({
                        object: obj
                }, 'getObject: done');
                callback(null, obj);
        });

        req.once('error', function (err) {
                log.debug(err, 'getObject: failed');
                callback(err);
        });
}


function deleteObject(client, bucket, key, options, callback) {
        assert.object(client, 'client');
        assert.string(bucket, 'bucket');
        assert.string(key, 'key');
        assert.object(options, 'options');
        assert.func(callback, 'callback');

        callback = once(callback);

        var opts = copyOptions(options);
        var log = utils.childLogger(client, opts);
        var req = client.rpc('delObject', bucket, key, opts);

        log.debug({
                bucket: bucket,
                key: key,
                etag: opts.etag
        }, 'deleteObject: entered');

        var cb = utils.simpleCallback({
                callback: callback,
                log: log,
                name: 'deleteObject',
                request: req
        });

        req.once('end', cb);
        req.once('error', cb);
}


function findObjects(client, bucket, filter, options) {
        assert.object(client, 'client');
        assert.string(bucket, 'bucket');
        assert.string(filter, 'filter');
        assert.object(options, 'options');

        var opts = copyOptions(options);
        var log = utils.childLogger(client, opts);
        var req = client.rpc('findObjects', bucket, filter, opts);
        var res = new EventEmitter;

        log.debug({
                bucket: bucket,
                filter: filter,
                options: opts
        }, 'findObjects: entered');
        req.on('message', function onObject(msg) {
                log.debug({
                        object: msg
                }, 'findObjects: record found');
                res.emit('record', msg);
        });

        req.once('end', function () {
                log.debug('findObjects: done');
                res.removeAllListeners('record');
                res.removeAllListeners('error');
                res.emit('end');
        });

        req.once('error', function (err) {
                log.debug(err, 'findObjects: failed');
                res.removeAllListeners('record');
                res.removeAllListeners('end');
                res.emit('error', err);
        });

        return (res);
}


function batchPut(client, requests, options, callback) {
        assert.object(client, 'client');
        assert.arrayOfObject(requests, 'requests');
        assert.object(options, 'options');
        assert.func(callback, 'callback');
        for (var i = 0; i < requests.length; i++) {
                var r = requests[i];
                assert.string(r.bucket, 'requests[' + i + '].bucket');
                assert.string(r.key, 'requests[' + i +'].key');
                assert.object(r.value, 'requests[' + i + '].value');
                assert.optionalObject(r.options, 'requests[' + i +'].options');
                r = (r.options || {}).headers;
                assert.optionalObject(r, 'requests[' + i + '].options.headers');
        }

        callback = once(callback);

        var meta;
        var opts = copyOptions(options);
        var log = utils.childLogger(client, opts);
        var req = client.rpc('batchPutObject', requests, opts);

        log.debug({
                requests: requests
        }, 'batchPutObject: entered');

        req.once('message', function (msg) {
                meta = msg;
        });

        req.once('end', function () {
                log.debug('batchPutObject: done');
                callback(null, meta || {});
        });

        req.once('error', function (err) {
                log.debug(err, 'batchPutObject: failed');
                callback(err);
        });
}



///--- Exports

module.exports = {
        putObject: putObject,
        getObject: getObject,
        deleteObject: deleteObject,
        findObjects: findObjects,
        batchPut: batchPut
};