// Copyright 2012 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;

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


function copyOptions(options, value) {
    var opts = {
        etag: options.etag !== undefined ? options.etag : options._etag,
        hashkey: options.hashkey,
        headers: options.headers || {},
        limit: options.limit,
        noCache: true,
        offset: options.offset,
        req_id: options.req_id || libuuid.create(),
        sort: options.sort,
        timeout: options.timeout,
        token: options.token,
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


function batch(client, requests, options, callback) {
    assert.object(client, 'client');
    assert.arrayOfObject(requests, 'requests');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    for (var i = 0; i < requests.length; i++) {
        var r = requests[i];
        var _s = 'requests[' + i + ']';
        assert.string(r.bucket, _s + '.bucket');
        assert.optionalObject(r.options, _s + '.options');
        assert.optionalString(r.operation, _s + '.operation');
        if (r.operation === 'update') {
            assert.object(r.fields, _s + '.fields');
            assert.string(r.filter, _s + '.filter');
        } else if (r.operation === 'delete') {
            assert.string(r.key, _s + '.key');
        } else if (r.operation === 'deleteMany') {
            assert.string(r.filter, _s + '.filter');
        } else {
            r.operation = r.operation || 'put';
            assert.equal(r.operation, 'put');
            assert.string(r.key, _s + '.key');
            assert.object(r.value, _s + '.value');
            assert.optionalString(r._value, _s + '._value');

            // Dirty to hack this in here, but meh
            if (!r._value)
                r._value = JSON.stringify(r.value);

            r = (r.options || {}).headers;
            assert.optionalObject(r, _s + '.options.headers');

        }

    }

    callback = once(callback);

    var meta;
    var opts = copyOptions(options);
    var log = utils.childLogger(client, opts);
    var req = client.rpc('batch', requests, opts);

    log.debug({
        requests: requests
    }, 'batch: entered');

    req.once('message', function (msg) {
        meta = msg;
    });

    req.once('end', function () {
        log.debug('batch: done');
        callback(null, meta || {});
    });

    req.once('error', function (err) {
        log.debug(err, 'batch: failed');
        callback(err);
    });
}


function updateObjects(client, bucket, fields, filter, options, callback) {
    assert.object(client, 'client');
    assert.string(bucket, 'bucket');
    assert.object(fields, 'fields');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    callback = once(callback);

    var opts = copyOptions(options);
    var log = utils.childLogger(client, opts);
    var meta;
    var req = client.rpc('updateObjects', bucket, fields, filter, opts);

    log.debug({
        bucket: bucket,
        fields: fields,
        filter: filter
    }, 'updateObjects: entered');

    req.once('message', function (obj) {
        meta = obj;
    });

    req.once('end', function () {
        log.debug({meta: meta}, 'updateObjects: done');
        callback(null, meta || {});
    });

    req.once('error', function (err) {
        log.debug(err, 'updateObjects: failed');
        callback(err);
    });
}


function deleteMany(client, bucket, filter, options, callback) {
    assert.object(client, 'client');
    assert.string(bucket, 'bucket');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    callback = once(callback);

    var opts = copyOptions(options);
    var log = utils.childLogger(client, opts);
    var req = client.rpc('deleteMany', bucket, filter, opts);
    log.debug({

        bucket: bucket,
        filter: filter
    }, 'deleteMany: entered');

    var meta;
    req.once('message', function (obj) {
        meta = obj;
    });

    req.once('end', function () {
        log.debug('deleteMany: done');
        callback(null, meta || {});
    });

    req.once('error', function (err) {
        log.debug(err, 'deleteMany: failed');
        callback(err);
    });
}



///--- Exports

module.exports = {
    putObject: putObject,
    getObject: getObject,
    deleteObject: deleteObject,
    findObjects: findObjects,
    batch: batch,
    updateObjects: updateObjects,
    deleteMany: deleteMany
};
