/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
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
var uuidv4 = require('uuid/v4');
var VError = require('verror');

var rpc = require('./rpc');


///--- API

function putObject(rpcctx, bucket, key, value, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(key, 'key');
    assert.object(value, 'value');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options, value);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'putObject',
        'rpcargs': [ bucket, key, value, opts ]
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

function getObject(rpcctx, bucket, key, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(key, 'key');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    var handledOptions = {
        'requireOnlineReindexing': false
    };
    var optionsSpec = {
        requireOnlineReindexing: {
            testNeedHandling: function testNeedHandling(value) {
                return (value === true);
            }
        }
    };

    var optionsToHandle = getOptionsToHandle(options, optionsSpec);
    var needMetadataRecord = optionsToHandle.length > 0;

    opts = makeOptions(options);
    if (needMetadataRecord) {
        opts.internalOpts = { sendHandledOptions: true };
    }

    function checkHandledOptions(metadata, obj) {
        var unhandledOptions;

        if (!jsprim.hasKey(metadata, '_handledOptions')) {
            callback(new VError({
                info: {
                    records: {
                        metadata: metadata,
                        obj: obj
                    }
                },
                message: 'received 2 data messages, but ' +
                    'first message does not look like a metadata record'
            }));
            return;
        }

        if (metadata._handledOptions) {
            if (metadata._handledOptions.indexOf('requireOnlineReindexing')
                !== -1) {
                handledOptions.requireOnlineReindexing = true;
            }
        }

        unhandledOptions = getUnhandledOptions(handledOptions, optionsToHandle);
        if (unhandledOptions.length > 0) {
            callback(createUnhandledOptionsError(unhandledOptions));
            return;
        }

        callback(null, obj);
    }

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'getObject',
        'rpcargs': [ bucket, key, opts ]
    }, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        if (data.length === 1) {
            if (needMetadataRecord) {
                err = createUnhandledOptionsError(Object.keys(handledOptions));
                callback(err);
                return;
            }

            callback(null, data[0]);
        } else if (data.length === 2) {
            checkHandledOptions(data[0], data[1]);
        } else {
            callback(new VError('expected 1 or 2 data messages, found %d',
                data.length));
        }
    });
}

function getShard(rpcctx, bucket, key, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(key, 'key');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'getShard',
        'rpcargs': [ bucket, key, opts ]
    }, function (err, data) {
        if (!err && data.length != 1) {
            err = new VError('expected exactly 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data[0]);
        }
    });
}


function deleteObject(rpcctx, bucket, key, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(key, 'key');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);

    /*
     * electric-moray sends trailing null values with this response.  These are
     * not normally allowed unless we specify ignoreNullValues.
     */
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'ignoreNullValues': true,
        'rpcmethod': 'delObject',
        'rpcargs': [ bucket, key, opts ]
    }, function (err, data) {
        /*
         * The server provides data in a response, but historically this client
         * ignores it.
         */
        callback(err);
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

function findObjects(rpcctx, bucket, filter, options) {
    var opts, log, req, res;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.optionalBool(options.requireIndexes, 'options.requireIndexes');

    var scheduleEmitUnhandledOptionsError = false;
    var gotMetadataRecord = false;
    var isFirstDataRecord = true;
    var needMetadataRecord = false;
    var handledOptions = {
        'requireIndexes': false,
        'requireOnlineReindexing': false
    };
    var optionsSpec = {
        requireIndexes: {
            testNeedHandling: function testRiNeedHandling(value) {
                return (value === true);
            }
        },
        requireOnlineReindexing: {
            testNeedHandling: function testRorNeedHandling(value) {
                return (value === true);
            }
        }
    };
    var optionsToHandle = [];
    var unhandledOptions = [];
    var unhandledOptionsErrorEmitted = false;

    optionsToHandle = getOptionsToHandle(options, optionsSpec);
    needMetadataRecord = optionsToHandle.length > 0;

    opts = makeOptions(options);
    if (needMetadataRecord) {
        opts.internalOpts = {sendHandledOptions: true};
    }

    log = rpc.childLogger(rpcctx, opts);
    res = new EventEmitter();
    req = rpc.rpcCommon({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'findObjects',
        'rpcargs': [ bucket, filter, opts ]
    }, function (err) {
        if (err) {
            if (VError.hasCauseWithName(err, 'FastRequestAbandonedError') &&
                scheduleEmitUnhandledOptionsError === true) {
                err = createUnhandledOptionsError(unhandledOptions, err);
            }

            res.emit('error', err);
        } else {
            if (!unhandledOptionsErrorEmitted) {
                if (needMetadataRecord && !gotMetadataRecord) {
                    /*
                     * Emit an 'UnhandledOptionsError' even if the request was
                     * successful but no data record was sent as part of the
                     * response, because in this case the client expected at
                     * least one data record to acknowledge which options were
                     * handled.
                     */
                    err = createUnhandledOptionsError(unhandledOptions);
                    res.emit('error', err);
                } else {
                    res.emit('end');
                }
            }
        }

        res.emit('_moray_internal_rpc_done');
    });

    req.on('data', function onObject(msg) {
        if (isFirstDataRecord && needMetadataRecord) {
            if (Object.hasOwnProperty.call(msg, '_handledOptions')) {
                gotMetadataRecord = true;

                if (msg._handledOptions) {
                    if (msg._handledOptions.indexOf('requireIndexes') !== -1) {
                        handledOptions.requireIndexes = true;
                    }
                    if (msg._handledOptions.indexOf('requireOnlineReindexing')
                        !== -1) {
                        handledOptions.requireOnlineReindexing = true;
                    }
                }
            }

            unhandledOptions = getUnhandledOptions(handledOptions,
                optionsToHandle);
            if (unhandledOptions.length > 0) {
                scheduleEmitUnhandledOptionsError = true;
                req.abandon();
                /*
                 * Make sure we don't emit a 'record' event after emitting an
                 * "UnhandledOptionsError" error.
                 */
                req.removeListener('data', onObject);
            }

            isFirstDataRecord = false;
        } else {
            log.debug({ object: msg }, 'findObjects: record found');
            res.emit('record', msg);
        }
    });

    return (res);
}

function batch(rpcctx, requests, options, callback) {
    assert.object(rpcctx, 'rpcctx');
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

            // Allowing differences between the 'value' and '_value' fields is
            // a recipe for disaster.  Any bucket with pre-update actions will
            // wipe out '_value' with a freshly stringified version.  If
            // '_value' contains an invalid JSON string, older version of moray
            // will still accept it, leading to errors when JSON parsing is
            // attempted later during get/find actions.
            // Once it can be ensured that all accessed morays are of an
            // appropriately recent version, this should be removed.
            assert.optionalString(r._value, _s + '._value');
            if (!r._value)
                r._value = JSON.stringify(r.value);

            r = (r.options || {}).headers;
            assert.optionalObject(r, _s + '.options.headers');
        }
    }

    var opts, log;

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'batch',
        'rpcargs': [ requests, opts ]
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

function updateObjects(rpcctx, bucket, fields, filter, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.object(fields, 'fields');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'updateObjects',
        'rpcargs': [ bucket, fields, filter, opts ]
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

function deleteMany(rpcctx, bucket, filter, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.string(filter, 'filter');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'deleteMany',
        'rpcargs': [ bucket, filter, opts ]
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

function reindexObjects(rpcctx, bucket, count, options, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(bucket, 'bucket');
    assert.number(count, 'count');
    assert.ok(count > 0, 'count > 0');
    assert.object(options, 'options');
    assert.func(callback, 'callback');

    opts = makeOptions(options);
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'reindexObjects',
        'rpcargs': [ bucket, count, opts ]
    }, function (err, data) {
        if (!err && data.length != 1) {
            err = new VError('expected exactly 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            var result = data[0];
            log.debug({ 'processed': result.processed },
                'reindexObjects: processed');
            callback(null, result);
        }
    });
}


///--- Helpers

function makeOptions(options, value) {
    var opts = jsprim.deepCopy(options);

    // Defaults handlers
    opts.req_id = options.req_id || uuidv4();
    opts.etag = (options.etag !== undefined) ? options.etag : options._etag;
    opts.headers = options.headers || {};
    opts.no_count = options.no_count || false;
    opts.sql_only = options.sql_only || false;
    opts.noCache = true;

    // Including the stringified value is redundant, but older versions of
    // moray depend upon the _value field being populated in this way.
    if (value)
        opts._value = JSON.stringify(value);

    if (typeof (options.noCache) !== 'undefined')
        opts.noCache = options.noCache;

    return (opts);
}


///--- Exports

module.exports = {
    putObject: putObject,
    getObject: getObject,
    getShard: getShard,
    deleteObject: deleteObject,
    findObjects: findObjects,
    batch: batch,
    updateObjects: updateObjects,
    deleteMany: deleteMany,
    reindexObjects: reindexObjects
};
