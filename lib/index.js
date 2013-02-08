// Copyright 2012 Joyent, Inc.  All rights reserved.

var url = require('url');

var assert = require('assert-plus');

var Client = require('./client').Client;
var clone = require('./utils').clone;



///-- API

module.exports = {
        Client: Client,

        createClient: function createClient(options) {
                assert.object(options, 'options');

                var opts = clone(options);
                opts.log = options.log;
                if (opts.url && !opts.host) {
                        var _u = url.parse(opts.url);
                        opts.host = _u.hostname;
                        opts.port = parseInt(opts.port || _u.port || 2020, 10);
                        delete opts.url;
                }

                return (new Client(opts));
        }
};
