// Copyright 2012 Joyent, Inc.  All rights reserved.

var client = require('./client');



module.exports = {
        Client: client.Client,

        createClient: function createClient(options) {
                return (new client.Client(options));
        }
};
