// Copyright 2012 Joyent, Inc.  All rights reserved.

if (require.cache[__dirname + '/helper.js'])
        delete require.cache[__dirname + '/helper.js'];
var helper = require('./helper.js');



///--- Globals

var test = helper.test;



///--- Tests

test('stub', function (t) {
        t.end();
});