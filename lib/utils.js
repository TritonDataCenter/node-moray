// Copyright 2012 Joyent, Inc.  All rights reserved.

var assert = require('assert-plus');
var once = require('once');
var uuid = require('node-uuid');



///--- API

function childLogger(client, options) {
        var log = client.log.child({
                req_id: (options || {}).req_id || uuid.v1()
        }, true);

        return (log);
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
        simpleCallback: simpleCallback
};