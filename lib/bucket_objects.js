/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/objects.js: object-related client API functions.  These functions are
 * invoked by same-named methods in lib/client.js to do the bulk of the work
 * associated with making RPC requests.  The arguments and semantics of these
 * functions are documented in the Moray API.
 */

var EventEmitter = require('events').EventEmitter;

var assert = require('assert-plus');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var VError = require('verror');

var rpc = require('./rpc');


///--- API

function putObject(rpcctx, owner, bucket_id, name, content_length, content_md5,
    content_type, headers, sharks, props, vnode, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(content_length, 'content_length');
    assert.string(content_md5, 'content_md5');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.object(sharks, 'sharks');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                vnode: vnode,
                content_length: content_length,
                content_md5: content_md5,
                content_type: content_type,
                headers: headers,
                sharks: sharks,
                properties: props
              };
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'putobject',
        'rpcargs': [arg]
    }, function (err, data) {
        if (!err && data.length > 1) {
            err = new VError('expected at most 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data.length === 0 ? {} : data[0]);
        }
    });
}

function putObjectNoVnode(rpcctx, owner, bucket_id, name, content_length, content_md5,
    content_type, headers, sharks, props, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(content_length, 'content_length');
    assert.string(content_md5, 'content_md5');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.object(sharks, 'sharks');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var args = [ owner,
                 bucket_id,
                 name,
                 content_length,
                 content_md5,
                 content_type,
                 headers,
                 sharks,
                 props
              ];
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'putobject',
        'rpcargs': args
    }, function (err, data) {
        if (!err && data.length > 1) {
            err = new VError('expected at most 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data.length === 0 ? {} : data[0]);
        }
    });
}


function getObject(rpcctx, owner, bucket_id, name, vnode, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                vnode: vnode
              };

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'getobject',
        'rpcargs': [arg]
    }, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        if (data.length === 1) {
            callback(null, data[0]);
        } else {
            callback(new VError('expected 1 data messages, found %d',
                data.length));
        }
    });
}

function getObjectNoVnode(rpcctx, owner, bucket_id, name, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var args = [ owner,
                 bucket_id,
                 name
               ];

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'getobject',
        'rpcargs': args
    }, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        if (data.length === 1) {
            callback(null, data[0]);
        } else {
            callback(new VError('expected 1 data messages, found %d',
                data.length));
        }
    });
}


function deleteObject(rpcctx, owner, bucket_id, name, vnode, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                vnode: vnode
              };

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'deleteobject',
        'rpcargs': [arg]
    }, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        if (data.length === 1) {
            callback(null, data[0]);
        } else {
            callback(new VError('expected 1 data messages, found %d',
                data.length));
        }
    });
}

function deleteObjectNoVnode(rpcctx, owner, bucket_id, name, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var args = [ owner,
                 bucket_id,
                 name
               ];

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'deleteobject',
        'rpcargs': args
    }, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        callback(null, data);
    });
}

/*
 * Returns an array of strings representing the name of options that should have
 * been explicitly marked as handled by a moray server, but were not. Returns an
 * empty array in case this set is empty.
 *
 * @param {Object} handledOptions - represents the options that were actually
 * acknowledged as handled by the Moray server that served this findObjects
 * request. If an option was acknowledged as handled by the server, then a
 * property with the name of the option will have the value true in the
 * "handledOptions" object.
 *
 * @param {string[]} optionsToHandle - represents the options that are expected
 * to be handled by the Moray server that served this findObjects request.
 */
function getUnhandledOptions(handledOptions, optionsToHandle) {
    assert.object(handledOptions, 'handledOptions');
    assert.arrayOfString(optionsToHandle, 'optionsToHandle');

    var optionName;
    var optionNameIndex;
    var unhandledOptions = [];

    for (optionNameIndex in optionsToHandle) {
        optionName = optionsToHandle[optionNameIndex];
        if (!handledOptions[optionName]) {
            unhandledOptions.push(optionName);
        }
    }

    return (unhandledOptions);
}

/*
 * Based on the option specifications in "optionsSpec", returns the option names
 * present in "options" that require an acknowledgement from Moray that they've
 * been handled.
 *
 * @param {Object} options
 * @param {Object} optionsSpec - stores metadata about some findObjects options,
 * such as a function named "testNeedHandling"
 */
function getOptionsToHandle(options, optionsSpec) {
    assert.object(options, 'options');
    assert.object(optionsSpec, 'optionsSpec');

    var optionName;
    var optionValue;
    var optionsToHandle = [];
    var testNeedHandlingFn;

    for (optionName in options) {
        if (!Object.hasOwnProperty.call(options, optionName)) {
            continue;
        }

        if (optionsSpec[optionName] !== undefined) {
            assert.object(optionsSpec[optionName], 'optionsSpec[optionName]');
            testNeedHandlingFn = optionsSpec[optionName].testNeedHandling;
            assert.func(testNeedHandlingFn, 'testNeedHandlingFn');

            optionValue = options[optionName];
            if (testNeedHandlingFn(optionValue)) {
                optionsToHandle.push(optionName);
            }
        }
    }

    return (optionsToHandle);
}

/*
 * Creates and returns a VError instance that represents an error due to the
 * Moray server not handling options that were expected to be handled by the
 * client.
 *
 * @param {Array} unhandledOptions - an array of strings that represents the
 * name of options that should have been marked as explicitly handled by the
 * moray server serving a findObjects request.
 *
 * @param {Object} cause - an instance of Error that will be used as the "cause"
 * for the newly created VError object.
 */
function createUnhandledOptionsError(unhandledOptions, cause) {
    assert.arrayOfString(unhandledOptions, 'unhandledOptions');
    assert.optionalObject(cause, 'cause');

    var err = new VError({
        name: 'UnhandledOptionsError',
        info: {
            unhandledOptions: unhandledOptions
        },
        cause: cause
    }, 'Unhandled options: %j', unhandledOptions);

    return (err);
}


///--- Helpers

function makeOptions(options, value) {
    var opts = jsprim.deepCopy(options);

    // Defaults handlers
    opts.req_id = options.req_id || libuuid.create();
    // opts.etag = (options.etag !== undefined) ? options.etag : options._etag;
    // opts.headers = options.headers || {};
    // opts.no_count = options.no_count || false;
    // opts.sql_only = options.sql_only || false;
    // opts.noCache = true;

    // Including the stringified value is redundant, but older versions of
    // moray depend upon the _value field being populated in this way.
    // if (value)
    //     opts._value = JSON.stringify(value);

    // if (typeof (options.noCache) !== 'undefined')
    //     opts.noCache = options.noCache;

    return (opts);
}


///--- Exports

module.exports = {
    putObject: putObject,
    getObject: getObject,
    deleteObject: deleteObject,
    putObjectNoVnode: putObjectNoVnode,
    getObjectNoVnode: getObjectNoVnode,
    deleteObjectNoVnode: deleteObjectNoVnode
};
