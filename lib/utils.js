// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var libuuid = require('libuuid');
var once = require('once');



///--- API

function childLogger(client, options) {
    var log = client.log.child({
        req_id: (options || {}).req_id || libuuid.create()
    }, true);

    return (log);
}


function clone(obj) {
    if (!obj) {
        return (obj);
    }
    var copy = {};
    Object.keys(obj).forEach(function (k) {
        copy[k] = obj[k];
    });
    return (copy);
}


function simpleCallback(opts) {
    assert.object(opts, 'options');
    assert.func(opts.callback, 'options.callback');
    assert.object(opts.log, 'options.log');
    assert.string(opts.name, 'options.name');
    assert.object(opts.request, 'options.request');

    function callback(err) {
        if (err) {
            opts.log.debug(err, '%s failed', opts.name);
            opts.callback(err);
            return;
        }

        opts.log.debug('%s done', opts.name);
        opts.request.removeAllListeners('end');
        opts.request.removeAllListeners('error');
        opts.callback();
    }

    return (callback);
}



///--- Exports

module.exports = {
    childLogger: childLogger,
    clone: clone,
    simpleCallback: simpleCallback
};