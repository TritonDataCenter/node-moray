/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * lib/client.js: Moray client implementation.  The MorayClient object is the
 * handle through which consumers make RPC requests to a remote Moray server.
 */

var EventEmitter = require('events').EventEmitter;
var net = require('net');
var path = require('path');
var url = require('url');
var util = require('util');

var assert = require('assert-plus');
var cueball = require('cueball');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var VError = require('verror');

var MorayConnectionPool = require('./pool');
var FastConnection = require('./fast_connection');
var buckets = require('./bucket_buckets');
var meta = require('./meta');
var objects = require('./bucket_objects');
var tokens = require('./tokens');
var parseMorayParameters = require('./client_params').parseMorayParameters;


///--- Default values for function arguments

var fastNRecentRequests         = 5;
var dflClientTcpKeepAliveIdle   = 10000;  /* milliseconds */

/*
 * See MorayClient() constructor.
 */
var MORAY_CS_OPEN    = 'open';
var MORAY_CS_CLOSING = 'closing';
var MORAY_CS_CLOSED  = 'closed';


///--- Helpers

function emitUnavailable() {
    var emitter = new EventEmitter();
    setImmediate(function () {
        emitter.emit('error', new Error('no active connections'));
    });
    return (emitter);
}


///--- API

/*
 * Constructor for the moray client.
 *
 * This client uses the cueball module to maintain a pool of TCP connections to
 * the IP addresses associated with a DNS name.  cueball is responsible for
 * DNS resolution (periodically, in the background) and maintaining the
 * appropriate TCP connections, while we maintain a small abstraction for
 * balancing requests across connections.
 *
 * The options accepted, the constraints on them, and several examples are
 * described in the moray(3) manual page inside this repository.  Callers can
 * also specify any number of legacy options documented with
 * populateLegacyOptions().
 */
function MorayClient(options) {
    var self = this;
    var coptions, cueballOptions, resolverInput;
    var resolver;

    EventEmitter.call(this);

    assert.object(options, 'options');
    assert.optionalObject(options.collector, 'options.collector');
    assert.object(options.log, 'options.log');
    assert.optionalBool(options.unwrapErrors, 'options.unwrapErrors');
    assert.optionalBool(options.failFast, 'options.failFast');
    assert.optionalBool(options.requireIndexes, 'options.requireIndexes');
    assert.optionalBool(options.requireOnlineReindexing,
        'options.requireOnlineReindexing');

    coptions = parseMorayParameters(options);
    cueballOptions = coptions.cueballOptions;

    /* Read-only metadata used for toString() and the like. */
    this.hostLabel = coptions.label;

    this.unwrapErrors = options.unwrapErrors ? true : false;
    this.failFast = options.failFast ? true : false;
    this.requireIndexes = options.requireIndexes ? true : false;
    this.requireOnlineReindexing =
        options.requireOnlineReindexing ? true : false;

    /* Helper objects. */
    this.log = options.log.child({
        component: 'MorayClient',
        domain: cueballOptions.domain
    }, true);

    this.log.debug(coptions, 'init');

    /* Optional artedi metrics collector that we'll pass to fast, if set. */
    if (options.collector) {
        this.collector = options.collector;
    }

    if (coptions.mode == 'srv') {
        resolverInput = cueballOptions.domain;
    } else {
        resolverInput = cueballOptions.domain + ':' +
            cueballOptions.defaultPort;
    }

    resolver = cueball.resolverForIpOrDomain({
        'input': resolverInput,
        'resolverConfig': {
            'resolvers': cueballOptions.resolvers,
            'recovery': cueballOptions.recovery,
            'service': cueballOptions.service,
            'defaultPort': cueballOptions.defaultPort,
            'maxDNSConcurrency': cueballOptions.maxDNSConcurrency,
            'log': this.log.child({ 'component': 'CueballResolver' }, true)
        }
    });
    if (resolver instanceof Error) {
        throw new VError(resolver, 'invalid moray client configuration');
    }
    resolver.start();
    this.cueballResolver = resolver;

    this.cueball = new cueball.ConnectionSet({
        'constructor': function cueballConstructor(backend) {
            return (self.createFastConnection(backend));
        },
        'log': this.log.child({ 'component': 'CueballSet' }, true),
        'resolver': resolver,
        'recovery': cueballOptions.recovery,
        'target': cueballOptions.target,
        'maximum': cueballOptions.maximum,
        'connectionHandlesError': true
    });

    /* Internal state. */
    this.nactive = 0;           /* count of outstanding RPCs */
    this.timeConnected = null;  /* time when first cueball conn established */
    this.ncontexts = 0;         /* counter of contexts ever created */
    this.activeContexts = {};   /* active RPC contexts (requests) */
    this.timeCueballInitFailed = null;   /* cueball entered "failed" */

    /*
     * State recorded when close() is invoked.  The closeState is one of:
     *
     *     MORAY_CS_OPEN        close() has never been invoked
     *
     *     MORAY_CS_CLOSING     close() has been invoked, but we have not
     *                          finished closing (presumably because outstanding
     *                          requests have not yet aborted)
     *
     *     MORAY_CS_CLOSED      close process has completed and there are no
     *                          connections in use any more.
     */
    this.closeState = MORAY_CS_OPEN;    /* see above */
    this.nactiveAtClose = null;         /* value of "nactive" at close() */

    /*
     * If requested, add a handler to ensure that the process does not exit
     * without this client being closed.
     */
    if (options.mustCloseBeforeNormalProcessExit) {
        this.onprocexit = function processExitCheck(code) {
            if (code === 0) {
                throw (new Error('process exiting before moray client closed'));
            }
        };
        process.on('exit', this.onprocexit);
    } else {
        this.onprocexit = null;
    }

    this.pool = new MorayConnectionPool({
        'log': this.log,
        'cueballResolver': this.cueballResolver,
        'cueballSet': this.cueball
    });

    this.cueballOnStateChange = function (st) {
        self.onCueballStateChange(st);
    };

    this.cueball.on('stateChanged', this.cueballOnStateChange);
}

util.inherits(MorayClient, EventEmitter);

/*
 * This getter is provided for historical reasons.  It's not a great interface.
 * It's intrinsically racy (i.e., the state may change as soon as the caller has
 * checked it), and it doesn't reflect much about the likelihood of a request
 * succeeding.  It's tempting to simply always report "true" so that clients
 * that might be tempted to avoid making a request when disconnected would just
 * go ahead and try it (which will fail quickly if we are disconnected anyway).
 * But we settle on the compromise position of reporting only whether we've
 * _ever_ had a connection.  This accurately reflects what many clients seem
 * interested in, which is whether we've set up yet.
 */
Object.defineProperty(MorayClient.prototype, 'connected', {
    'get': function () {
        return (this.timeConnected !== null &&
            this.closeState == MORAY_CS_OPEN);
    }
});

/*
 * During startup, we wait for the state of the Cueball ConnectionSet to reach
 * "running", at which point we emit "connect" so that callers know that they
 * can start using this client.
 *
 * If "failFast" was specified in the constructor, then if the ConnectionSet
 * reaches "failed" before "connected", then we emit an error and close the
 * client.  See the "failFast" documentation above for details.
 */
MorayClient.prototype.onCueballStateChange = function onCueballStateChange(st) {
    var err;

    assert.strictEqual(this.timeConnected, null);
    assert.strictEqual(this.timeCueballInitFailed, null);

    this.log.trace({ 'newState': st }, 'cueball state change');

    if (st == 'running') {
        this.timeConnected = new Date();
        this.cueball.removeListener('stateChanged', this.cueballOnStateChange);
        this.log.debug('client ready');
        this.emit('connect');
    } else if (this.failFast && st == 'failed') {
        this.timeCueballInitFailed = new Date();
        this.cueball.removeListener('stateChanged', this.cueballOnStateChange);
        err = new VError('moray client "%s": failed to establish connection',
            this.hostLabel);
        this.log.warn(err);
        this.emit('error', err);
        this.close();
    }
};

/**
 * Aborts outstanding requests, shuts down all connections, and closes this
 * client down.
 */
MorayClient.prototype.close = function close() {
    var self = this;

    if (this.closeState != MORAY_CS_OPEN) {
        this.log.warn({
            'closeState': this.closeState,
            'nactiveAtClose': this.nactiveAtClose
        }, 'ignoring close() after previous close()');
        return;
    }

    if (this.onprocexit !== null) {
        process.removeListener('exit', this.onprocexit);
        this.onprocexit = null;
    }

    this.closeState = MORAY_CS_CLOSING;
    this.nactiveAtClose = this.nactive;
    this.log.info({ 'nactiveAtClose': this.nactive }, 'closing');
    this.pool.fallbackDisable();

    if (this.nactive === 0) {
        setImmediate(function closeImmediate() { self.closeFini(); });
        return;
    }

    /*
     * Although we would handle sockets destroyed underneath us, the most
     * straightforward way to clean up is to proactively terminate outstanding
     * requests, wait for them to finish, and then stop the set.  We do this by
     * detaching each underlying Fast client from its socket.  This should cause
     * Fast to fail any oustanding requests, causing the RPC contexts to be
     * released, and allowing us to proceed with closing.
     */
    jsprim.forEachKey(this.activeContexts, function (_, rpcctx) {
        rpcctx.fastClient().detach();
    });
};

MorayClient.prototype.closeFini = function closeFini() {
    assert.equal(this.closeState, MORAY_CS_CLOSING);
    assert.equal(this.nactive, 0);
    assert.ok(jsprim.isEmpty(this.activeContexts));

    var self = this;
    this.cueball.on('stateChanged', function (st) {
        if (st === 'stopped') {
            self.log.info('closed');
            self.emit('close');
        }
    });

    this.log.info('waiting for cueball to stop');
    this.cueball.stop();
    this.cueballResolver.stop();
};


MorayClient.prototype.toString = function toString() {
    var str = util.format('[object MorayClient<host=%s>]', this.hostLabel);
    return (str);
};

/*
 * Given a cueball "backend", return a Cueball-compatible Connection object.
 * This is implemented by the separate FastConnection class.
 */
MorayClient.prototype.createFastConnection =
    function createFastConnection(backend) {
    assert.string(backend.key, 'backend.key');
    assert.string(backend.name, 'backend.name');
    assert.string(backend.address, 'backend.address');
    assert.number(backend.port, 'backend.port');

    return (new FastConnection({
        'address': backend.address,
        'collector': this.collector,
        'port': backend.port,
        'nRecentRequests': fastNRecentRequests,
        'tcpKeepAliveInitialDelay': dflClientTcpKeepAliveIdle,
        'log': this.log.child({
            'component': 'FastClient',
            'backendName': backend.name
        })
    }));
};

/*
 * Internal functions for RPC contexts and context management
 *
 * Each RPC function receives as its first argument a MorayRpcContext, which is
 * a per-request handle for accessing configuration (like "unwrapErrors") and
 * the underlying Fast client.  When the RPC completes, the implementing
 * function must release the MorayRpcContext.  This mechanism enables us to
 * ensure that connections are never released twice from the same RPC, and it
 * also affords some debuggability if connections become leaked.  Additionally,
 * if future RPC function implementors need additional information from the
 * Moray client (e.g., a way to tell whether the caller has tried to cancel the
 * request), we can add additional functions to the MorayRpcContext.
 *
 * RPC functions use one of two patterns for obtaining and releasing RPC
 * contexts, depending on whether they're callback-based or event-emitter-based.
 * It's always possible for an RPC to fail because no RPC connections are
 * available, and these two mechanisms differ in how they handle that:
 *
 *    (1) Callback-based RPCs (e.g., getBucket) use this pattern:
 *
 *          rpcctx = this.ctxCreateForCallback(usercallback);
 *          if (rpcctx !== null) {
 *              callback = this.makeReleaseCb(rpcctx, usercallback);
 *              // Make the RPC call and invoke callback() upon completion.
 *          }
 *
 *        If a backend connection is available, a MorayRpcContext will be
 *        returned from ctxCreateForCallback().  These functions typically use
 *        makeReleaseCb() to wrap the user callback they were given with one
 *        that releases the RPC context before invoking the user callback.
 *
 *        If no backend connection is available, then callback() will be invoked
 *        asynchronously with an appropriate error, and the caller should not do
 *        anything else.
 *
 *    (2) Event-emitter-based RPCs (e.g., findObjects) use this pattern:
 *
 *          rpcctx = this.ctxCreateForEmitter();
 *          if (rpcctx !== null) {
 *              ee = new EventEmitter();
 *              this.releaseWhenDone(rpcctx, ee);
 *              // Make the RPC call and emit 'end' or 'error' upon completion.
 *          } else {
 *              ee = emitUnavailable();
 *          }
 *
 *          return (ee);
 *
 *        If a backend connection is available, a MorayRpcContext will be
 *        returned from ctxCreateForEmitter().  These functions typically use
 *        releaseWhenDone() to release the RPC context when the event emitter
 *        emits '_moray_internal_rpc_done'.
 *
 *        If no backend connection is available, then the caller is responsible
 *        for allocating and returning a new EventEmitter that will emit the
 *        appropriate Error.
 */

/*
 * Internal function that returns a context used for RPC operations for a
 * callback-based RPC call.  If no backend connection is available, this
 * function returns null and schedules an asynchronous invocation of the given
 * callback with a suitable error.
 *
 * See "Internal functions for RPC contexts and context management" above.
 */
MorayClient.prototype.ctxCreateForCallback =
    function ctxCreateForCallback(callback) {
    var conn;

    assert.func(callback, 'callback');
    if (this.closeState != MORAY_CS_OPEN) {
        setImmediate(callback, new Error('moray client has been closed'));
        return (null);
    }

    conn = this.pool.connAlloc();
    if (conn instanceof Error) {
        setImmediate(callback, conn);
        return (null);
    }

    return (this.ctxCreateCommon(conn));
};

/*
 * Internal function that returns a context used for RPC operations for an
 * event-emitter-based RPC call.  If no backend connection is available, this
 * function returns null and the caller is responsible for propagating the error
 * to its caller.
 *
 * See "Internal functions for RPC contexts and context management" above.
 */
MorayClient.prototype.ctxCreateForEmitter = function ctxCreateForEmitter() {
    var conn;

    if (this.closeState != MORAY_CS_OPEN) {
        return (null);
    }

    conn = this.pool.connAlloc();
    if (conn instanceof Error) {
        /* The caller knows that this means there are no connections. */
        return (null);
    }

    return (this.ctxCreateCommon(conn));
};

/*
 * Internal function that creates an RPC context wrapping the given connection.
 * We keep track of outstanding RPC contexts to provide a clean close()
 * implementation and to aid debuggability in the event of leaks.
 */
MorayClient.prototype.ctxCreateCommon = function (conn) {
    var rpcctx;

    assert.object(conn);
    assert.ok(!(conn instanceof Error));
    assert.equal(this.closeState, MORAY_CS_OPEN);

    this.nactive++;

    rpcctx = new MorayRpcContext({
        'id': this.ncontexts++,
        'morayClient': this,
        'connection': conn
    });

    assert.ok(!this.activeContexts.hasOwnProperty(rpcctx.mc_id));
    this.activeContexts[rpcctx.mc_id] = rpcctx;
    return (rpcctx);
};

/*
 * Internal function for releasing an RPC context (that is, releasing the
 * underlying connection).
 */
MorayClient.prototype.ctxRelease = function ctxRelease(rpcctx) {
    assert.ok(this.nactive > 0);
    this.nactive--;

    assert.equal(this.activeContexts[rpcctx.mc_id], rpcctx);
    delete (this.activeContexts[rpcctx.mc_id]);
    this.pool.connRelease(rpcctx.mc_conn);

    if (this.nactive === 0 && this.closeState == MORAY_CS_CLOSING) {
        this.closeFini();
    }
};

/*
 * Given an RPC context and a user callback, return a callback that will
 * release the underlying RPC context and then invoke the user callback with the
 * same arguments.
 *
 * See "Internal functions for RPC contexts and context management" above.
 */
MorayClient.prototype.makeReleaseCb = function makeReleaseCb(rpcctx, cb) {
    var self = this;
    return (function onCallbackRpcComplete() {
        self.ctxRelease(rpcctx);
        cb.apply(null, arguments);
    });
};

/*
 * Given an RPC context and an event emitter, return a callback that will
 * release the underlying RPC context when the event emitter emits "end" or
 * "error".  This is the EventEmitter analog of makeReleaseCb.
 *
 * See "Internal functions for RPC contexts and context management" above.
 */
MorayClient.prototype.releaseWhenDone = function releaseOnEnd(rpcctx, emitter) {
    var self = this;
    var done = false;

    assert.object(rpcctx);
    assert.object(emitter);
    assert.ok(emitter instanceof EventEmitter);

    emitter.on('_moray_internal_rpc_done', function onEmitterRpcComplete() {
        assert.ok(!done);
        done = true;
        self.ctxRelease(rpcctx);
    });
};

/*
 * RPC implementation functions
 *
 * These are the primary public methods on the Moray client.  Typically, these
 * functions normalize and validate their arguments and then delegate to an
 * implementation in one of the nearby files.  They use one of the patterns
 * described above under "Internal functions for RPC contexts and context
 * management" to manage the RPC context.
 */

/**
 * Creates a Bucket
 *
 * `cfg` allows you to pass in index information, as well as pre/post triggers.
 * See https://mo.joyent.com/docs/moray/master/#CreateBucket for more info.
 *
 * @param {String} b    - Bucket name
 * @param {Object} cfg  - configuration
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.createBucket = function createBucket(owner, bucket, vnode, cb) {
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.createBucket(rpcctx, owner, bucket, vnode,
            this.makeReleaseCb(rpcctx, cb));
};

MorayClient.prototype.createBucketNoVnode = function createBucketNoVnode(owner, bucket, cb) {
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.createBucketNoVnode(rpcctx, owner, bucket,
            this.makeReleaseCb(rpcctx, cb));
};


/**
 * Fetches a Bucket
 *
 * See https://mo.joyent.com/docs/moray/master/#GetBucket for more info.
 *
 * @param {String} b    - Bucket name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.getBucket = function getBucket(o, b, vnode, cb) {
    var opts = {};
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.getBucket(rpcctx, o, b, vnode, this.makeReleaseCb(rpcctx, cb));
};


MorayClient.prototype.getBucketNoVnode = function getBucketNoVnode(o, b, cb) {
    var opts = {};
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.getBucketNoVnode(rpcctx, o, b, this.makeReleaseCb(rpcctx, cb));
};

/**
 * Lists buckets
 */
MorayClient.prototype.listBuckets = function listBuckets(owner, vnode) {
    var rpcctx = this.ctxCreateForEmitter();
    var rv;

    if (!rpcctx) {
        return (emitUnavailable());
    }

    rv = buckets.listBuckets(rpcctx, owner, vnode);
    this.releaseWhenDone(rpcctx, rv);

    return (rv);
};

MorayClient.prototype.listBucketsNoVnode = function listBucketsNoVnode(owner) {
    var rpcctx = this.ctxCreateForEmitter();
    var rv;

    if (!rpcctx) {
        return (emitUnavailable());
    }

    rv = buckets.listBucketsNoVnode(rpcctx, owner);
    this.releaseWhenDone(rpcctx, rv);

    return (rv);
};


/**
 * Updates an existing Bucket
 *
 * `cfg` allows you to pass in index information, as well as pre/post triggers.
 * See https://mo.joyent.com/docs/moray/master/#UpdateBucket for more info.
 *
 * @param {String} b    - Bucket name
 * @param {Object} cfg  - configuration
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.updateBucket = function updateBucket(b, cfg, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.updateBucket(rpcctx, b, cfg, opts,
            this.makeReleaseCb(rpcctx, cb));
};


/**
 * Deletes a Bucket
 *
 * See https://mo.joyent.com/docs/moray/master/#DeleteBucket for more info.
 *
 * @param {String} b    - Bucket name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.deleteBucket = function deleteBucket(o, b, vnode, cb) {
    var opts = {};
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.deleteBucket(rpcctx, o, b, vnode, this.makeReleaseCb(rpcctx, cb));
};


MorayClient.prototype.deleteBucketNoVnode = function deleteBucketNoVnode(o, b, cb) {
    var opts = {};
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.deleteBucketNoVnode(rpcctx, o, b, this.makeReleaseCb(rpcctx, cb));
};


/**
 * Creates or replaces a Bucket.
 *
 * Note that this is actually just a client shim, and simply calls
 * get, followed by create || update.  This is not transactional,
 * and there are races, so you probably just want to call this once
 * at startup in your code.
 *
 * @param {String} b    - Bucket name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.putBucket = function putBucket(b, cfg, opts, cb) {
    assert.string(b, 'bucket');
    assert.object(cfg, 'config');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.putBucket(rpcctx, b, cfg, opts, this.makeReleaseCb(rpcctx, cb));
};

/**
 * Idempotently Creates or Replaces an Object.
 *
 * See https://mo.joyent.com/docs/moray/master/#PutObject for more info.
 *
 * @param {String} b    - Bucket name
 * @param {String} k    - Key name
 * @param {Object} v    - Value
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.putObject = function putObject(owner, bucket_id, name,
    content_length, content_md5, content_type, headers, sharks, props, vnode,
    cb) {
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(content_length, 'content_length');
    assert.string(content_md5, 'content_md5');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.object(sharks, 'sharks');
    assert.number(vnode, 'vnode');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.putObject(rpcctx, owner, bucket_id, name,
            content_length, content_md5, content_type, headers, sharks, props,
            vnode, this.makeReleaseCb(rpcctx, cb));
    }
};

MorayClient.prototype.putObjectNoVnode = function putObjectNoVnode(owner, bucket_id, name,
    content_length, content_md5, content_type, headers, sharks, props,
    cb) {
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(content_length, 'content_length');
    assert.string(content_md5, 'content_md5');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.object(sharks, 'sharks');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.putObjectNoVnode(rpcctx, owner, bucket_id, name,
            content_length, content_md5, content_type, headers, sharks, props,
            this.makeReleaseCb(rpcctx, cb));
    }
};


/**
 * Fetches an Object
 *
 * See https://mo.joyent.com/docs/moray/master/#GetObject for more info.
 *
 * @param {String} b    - Bucket name
 * @param {String} k    - Key name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.getObject = function getObject(owner, bucket_id, name, vnode, cb) {
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.getObject(rpcctx, owner, bucket_id, name, vnode,
            this.makeReleaseCb(rpcctx, cb));
    }
};

MorayClient.prototype.getObjectNoVnode = function getObjectNoVnode(owner, bucket_id, name, cb) {
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.getObjectNoVnode(rpcctx, owner, bucket_id, name,
            this.makeReleaseCb(rpcctx, cb));
    }
};


/**
 * Deletes an Object
 *
 * See https://mo.joyent.com/docs/moray/master/#DeleteObject for more info.
 *
 * @param {String} b    - Bucket name
 * @param {String} k    - Key name
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.deleteObject = function deleteObject(owner, bucket_id, name, vnode, cb) {
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.deleteObject(rpcctx, owner, bucket_id, name, vnode,
            this.makeReleaseCb(rpcctx, cb));
    }
};

MorayClient.prototype.deleteObjectNoVnode = function deleteObjectNoVnode(owner, bucket_id, name, cb) {
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.deleteObjectNoVnode(rpcctx, owner, bucket_id, name,
            this.makeReleaseCb(rpcctx, cb));
    }
};


/**
 * Finds object matching a given filter
 *
 * See https://mo.joyent.com/docs/moray/master/#FindObjects for more info.
 *
 * @param {String} b      - Bucket name
 * @param {String} f      - Object filter
 * @param {Object} opts   - request parameters
 * @return {EventEmitter} - listen for 'record', 'end' and 'error'
 */
MorayClient.prototype.findObjects = function findObjects(b, f, opts) {
    assert.string(b, 'bucket');
    assert.string(f, 'filter');
    assert.object(opts, 'options');
    assert.string(opts.hashkey, 'opts.hashkey');

    opts = jsprim.mergeObjects(opts, null, {
        requireIndexes: this.requireIndexes,
        requireOnlineReindexing: this.requireOnlineReindexing
    });

    var rpcctx = this.ctxCreateForEmitter();
    if (!rpcctx) {
        return (emitUnavailable());
    }

    /*
     * For manta buckets, we do some special work to break down the hash key
     * and figure out if the request is to list the buckets, or the contents of
     * a bucket.
     *
     * XXX I don't know if this is the best way to do this or the best place
     * for the logic but yolo.
     */
    var key = path.normalize(opts.hashkey);

    this.log.debug({key: key}, 'findObjects key: %s', key);

    assert.equal(key[0], '/', 'path begin with /');
    var parts = key.substr(1).split('/');
    var owner = parts[0];
    var bucket;

    assert.equal(parts[1], 'buckets', 'part 2 buckets');

    // check if user wants to list bucket contents
    if (parts[2]) {
        bucket = parts[2];
        assert.string(bucket, 'bucket');
        assert(bucket.length > 0, 'bucket.length > 0');

        assert.equal(parts[3], 'objects', 'part 3 objects');
        assert(!parts[4], 'extra garbage in path');
    }

    this.log.debug({parts: parts, bucket: bucket},
        'findObjects key parsed');

    var rv;

    if (bucket) {
        rv = objects.listObjectsNoVnode(rpcctx, owner, bucket);
    } else {
        rv = buckets.listBucketsNoVnode(rpcctx, owner);
    }

    this.releaseWhenDone(rpcctx, rv);
    return (rv);
};
MorayClient.prototype.find = MorayClient.prototype.findObjects;


/**
 * Idempotently Creates or Replaces a set of Object.
 *
 * See https://mo.joyent.com/docs/moray/master/#Batch for more info.
 *
 * @param {Array} requests - {bucket, key, value} tuples
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.batch = function batch(requests, opts, cb) {
    assert.arrayOfObject(requests, 'requests');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        objects.batch(rpcctx, requests, opts, this.makeReleaseCb(rpcctx, cb));
};


/**
 * Updates a set of object attributes.
 *
 * See https://mo.joyent.com/docs/moray/master/#UpdateObjects for more info.
 *
 * @param {String} bucket - bucket
 * @param {Object} fields - attributes to update (must be indexes)
 * @param {String} filter - update objects matching this filter
 * @param {Object} opts   - request parameters
 * @param {Function} cb   - callback
 */
MorayClient.prototype.updateObjects = function update(b, f, f2, opts, cb) {
    assert.string(b, 'bucket');
    assert.object(f, 'fields');
    assert.string(f2, 'filter');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        objects.updateObjects(rpcctx, b, f, f2, opts,
            this.makeReleaseCb(rpcctx, cb));
};


/**
 * Deletes a group of objects.
 *
 * See https://mo.joyent.com/docs/moray/master/#DeleteMany for more info.
 *
 * @param {String} bucket - bucket
 * @param {String} filter - update objects matching this filter
 * @param {Object} opts   - request parameters
 * @param {Function} cb   - callback
 */
MorayClient.prototype.deleteMany = function deleteMany(b, f, opts, cb) {
    assert.string(b, 'bucket');
    assert.string(f, 'filter');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        objects.deleteMany(rpcctx, b, f, opts, this.makeReleaseCb(rpcctx, cb));
};


/**
 * Request reindexing of stale rows.
 *
 * Returns a count of successfully processed rows.  Once the processed count
 * reaches zero, all rows will be properly reindexed.
 *
 * @param {String} bucket - bucket
 * @param {String} count  - max objects to reindex
 * @param {Object} opts   - request parameters
 * @param {Function} cb   - callback
 */
MorayClient.prototype.reindexObjects = function reindexObjects(b, c, opts, cb) {
    assert.string(b, 'bucket');
    assert.number(c, 'count');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        objects.reindexObjects(rpcctx, b, c, opts,
            this.makeReleaseCb(rpcctx, cb));
};


/*
 * Gets the set of tokens from moray.
 *
 * See https://mo.joyent.com/docs/moray/master/#UpdateObjects for more info.
 *
 * @param {Object} opts   - request parameters
 * @param {Function} cb   - callback
 */
MorayClient.prototype.getTokens = function getTokens(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        tokens.getTokens(rpcctx, opts, this.makeReleaseCb(rpcctx, cb));
};


/**
 * Performs a ping check against the server.
 *
 * Note that because the MorayClient is pooled and connects to all IPs in
 * a RR-DNS set, this actually just tells you that _one_ of the servers is
 * responding, not that all are.
 *
 * In most cases, you probably want to send in '{deep: true}' as options
 * so a DB-level check is performed on the server.
 *
 * @param {Object} opts   - request parameters
 * @return {EventEmitter} - listen for 'record', 'end' and 'error'
 */
MorayClient.prototype.ping = function _ping(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        meta.ping(rpcctx, opts, this.makeReleaseCb(rpcctx, cb));
};

/**
 * Query the API version of the server.
 *
 * Do not use this function except for reporting version numbers to humans.  See
 * the comment in meta.versionInternal().
 *
 * @param {Object} opts   - request parameters
 * @param {Function} cb   - callback
 */
MorayClient.prototype.versionInternal = function _version(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        meta.versionInternal(rpcctx, opts, this.makeReleaseCb(rpcctx, cb));
};

/**
 * Performs a raw SQL operation against the server.
 *
 * For the love of all that is good in this earth, please only use this method
 * at the utmost of need.  In almost every case this is used, it's used for
 * nefarious reasons.
 *
 * The only intended uses of this are for edge cases that require custom
 * "serial" tables, et al, in the database (like UFDS changelog).  You
 * absolutely do not ever need to use this if put/get/del/find works for you.
 *
 * @param {String} stmt   - SQL Statement
 * @param {Array} vals    - Values (if SQL statement has $1 etc. in it)
 * @param {Object} opts   - Request Options
 * @return {EventEmitter} - listen for 'record', 'end' and 'error'
 */
MorayClient.prototype.sql = function _sql(stmt, vals, opts) {
    var rv;

    switch (arguments.length) {
    case 0:
        throw new TypeError('statement (String) required');
    case 1:
        assert.string(stmt, 'statement');
        vals = [];
        opts = {};
        break;
    case 2:
        assert.string(stmt, 'statement');
        if (!Array.isArray(vals)) {
            assert.object(vals, 'options');
            opts = vals;
            vals = [];
        } else {
            opts = {};
        }
        break;
    case 3:
        assert.string(stmt, 'statement');
        assert.ok(Array.isArray(vals));
        assert.object(opts, 'options');
        break;
    default:
        throw new Error('too many arguments');
    }

    var rpcctx = this.ctxCreateForEmitter();
    if (rpcctx) {
        rv = meta.sql(rpcctx, stmt, vals, opts);
        this.releaseWhenDone(rpcctx, rv);
        return (rv);
    }
    return (emitUnavailable());
};


/*
 * A MorayRpcContext is a per-request handle that refers back to the Moray
 * client and the underlying connection.  This object is provided to RPC
 * implementors, and allows them to access the underlying Fast client (in order
 * to make RPC requests), configuration (like "unwrapErrors"), and to release
 * the RPC context when the RPC completes.
 *
 * This class should be thought of as part of the implementation of the Moray
 * client itself, having internal implementation knowledge of the client.
 */
function MorayRpcContext(args) {
    assert.object(args, 'args');
    assert.number(args.id, 'args.id');
    assert.object(args.connection, 'args.connection');
    assert.object(args.morayClient, 'args.morayClient');

    /*
     * There's no mechanism in place to stop us from reaching this limit, but
     * even at one million requests per second, we won't hit it until the client
     * has been running for over 142 years.
     */
    assert.ok(args.id >= 0 && args.id < Math.pow(2, 53));

    this.mc_id = args.id;
    this.mc_conn = args.connection;
    this.mc_moray = args.morayClient;
}

MorayRpcContext.prototype.fastClient = function fastClient() {
    return (this.mc_conn.connection().fastClient());
};

MorayRpcContext.prototype.socketAddrs = function socketAddrs() {
    return (this.mc_conn.connection().socketAddrs());
};

MorayRpcContext.prototype.unwrapErrors = function unwrapErrors() {
    assert.bool(this.mc_moray.unwrapErrors);
    return (this.mc_moray.unwrapErrors);
};

MorayRpcContext.prototype.createLog = function createLog(options) {
    var reqid;
    assert.optionalObject(options, 'options');

    if (options && options.req_id) {
        reqid = options.req_id;
    } else {
        reqid = libuuid.create();
    }

    return (this.mc_moray.log.child({ 'req_id': reqid }, true));
};


///--- Exports

/*
 * Expose privateParseMorayParameters privately for testing, not for the outside
 * world.
 */
MorayClient.privateParseMorayParameters = parseMorayParameters;

module.exports = {
    Client: MorayClient
};
