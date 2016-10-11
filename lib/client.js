/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * lib/client.js: Moray client implementation.  The MorayClient object is the
 * handle through which consumers make RPC requests to a remote Moray server.
 */

var EventEmitter = require('events').EventEmitter;
var net = require('net');
var url = require('url');
var util = require('util');

var assert = require('assert-plus');
var cueball = require('cueball');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var VError = require('verror');

var MorayConnectionPool = require('./pool');
var FastConnection = require('./fast_connection');
var buckets = require('./buckets');
var meta = require('./meta');
var objects = require('./objects');
var tokens = require('./tokens');


///--- Default values for function arguments

var dflClientTargetConnections = 6;
var dflClientMaxConnections = 15;
var dflClientMaxDnsConcurrency = 3;
var dflClientConnectRetries = 5;
var dflClientConnectTimeout = 2000;     /* milliseconds */
var dflClientDnsTimeout = 1000;         /* milliseconds */
var dflClientDnsDelayMin = 10;          /* milliseconds */
var dflClientDnsDelayMax = 10000;       /* milliseconds */
var dflClientDelayMin = 1000;           /* milliseconds */
var dflClientDelayMax = 60000;          /* milliseconds */

var fastNRecentRequests = 30;
var dflClientTcpKeepAliveIdle = 10000;  /* milliseconds */

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

/*
 * This function constructs a set of node-cueball arguments based on legacy
 * properties accepted by the MorayClient constructor.  Those properties
 * included the following required properties:
 *
 *     url               string describing the URL (host and port) to connect to
 *
 * or:
 *
 *     host              string IP address or DNS name for remote Moray service.
 *                       If this is an IP address, then DNS is not used.
 *                       Otherwise, the string is used as a DNS name to find the
 *                       actual IP addresses, and this list of IPs is maintained
 *                       via periodic re-resolution of the DNS name.
 *
 *     port              positive integer: TCP port for remote Moray service
 *
 * and the following optional properties that, if not specified, have defaults
 * that are assumed to be reasonable:
 *
 *     connectTimeout    non-negative, integer number of milliseconds
 *                       to wait for TCP connections to be established
 *
 *     dns (object)      describes DNS behavior
 *
 *     dns.checkInterval non-negative, integer number of milliseconds
 *                       between periodic resolution of DNS names used to keep
 *                       the set of connected IPs up to date.  This is not used
 *                       by cueball any more.
 *
 *     dns.resolvers     array of string IP addresses to use for DNS resolvers
 *
 *     dns.timeout       non-negative, integer number of milliseconds to wait
 *                       for DNS query responses
 *
 *     maxConnections    non-negative, integer number of TCP connections that
 *                       may ever be opened to each IP address used.  If "host"
 *                       is an IP address, then this is the maximum number of
 *                       connections, but if "host" is a DNS name, then there
 *                       may be up to "maxConnections" per remote IP found in
 *                       DNS.
 *
 *     retry (object)    describes a retry policy used for establishing
 *                       connections.  Historically, the behavior with respect
 *                       to this policy was confusing at best: this policy was
 *                       used for establishing TCP connections to remote
 *                       servers, but a second, hardcoded policy was used when
 *                       this first policy was exhausted.  This policy appears
 *                       to have been intended to cover DNS operations as well,
 *                       but was not actually used.  In the current
 *                       implementation, this policy is the one used for TCP
 *                       connection establishment, and callers wanting to
 *                       specify a DNS policy must specify cueball options
 *                       directly rather than using these legacy options.
 *
 *     retry.retries     non-negative, integer number of retry attempts.  It's
 *                       unspecified whether this is the number of attempts or
 *                       the number of retries (i.e., one fewer than the number
 *                       of attempts).  Today, this is interpreted by
 *                       node-cueball.  Historically, this was interpreted by
 *                       the node-backoff module.
 *
 *     retry.minTimeout  non-negative, integer number of milliseconds to wait
 *                       after the first operation failure before retrying
 *
 *     retry.maxTimeout  non-negative, integer representing the maximum number
 *                       of milliseconds between retries.  Some form of backoff
 *                       (likely exponential) is used to determine the delay,
 *                       but it will always be between retry.minTimeout and
 *                       retry.maxTimeout.
 *
 * Additional properties were at one time documented, but never used:
 * maxIdleTime and pingTimeout.
 */
function translateLegacyOptions(options) {
    var cueballOptions, r, u;
    var host, port;

    /*
     * This logic mirrors the legacy behavior of createClient, however
     * unnecessarily complicated.  Specifically:
     *
     *     host     comes from "options.host" if present, and otherwise from
     *              parsing "options.url"
     *
     *     port     comes from "options.port" (as a string or number) if
     *              present.  If "host" was not present and "url" is, then
     *              "port" MAY come from the parsed URL.  Otherwise, the port is
     *              2020.
     */
    if (typeof (options.url) == 'string' && !options.hasOwnProperty('host')) {
        u = url.parse(options.url);
        host = u.hostname;

        if (options.port) {
            port = options.port;
        } else if (u.port) {
            port = u.port;
        } else {
            port = 2020;
        }

        port = parseInt(port, 10);
        assert.ok(!isNaN(port), 'port must be a number');
    } else {
        host = options.host;
        port = options.port;
    }

    assert.string(host, 'options.host');
    assert.number(port, 'options.port');
    assert.optionalNumber(options.maxConnections, 'options.maxConnections');
    assert.optionalNumber(options.connectTimeout, 'options.connectTimeout');
    assert.optionalObject(options.retry, 'options.retry');
    assert.optionalObject(options.dns, 'options.dns');

    cueballOptions = {
        /* Resolver parameters */
        'domain': host,
        'maxDNSConcurrency': dflClientMaxDnsConcurrency,
        'defaultPort': port,

        /* ConnectionSet parameters */
        'target': dflClientTargetConnections,
        'maximum': options.maxConnections || dflClientMaxConnections,

        /* Shared parameters */
        'recovery': {}
    };

    if (cueballOptions.maximum < cueballOptions.target) {
        cueballOptions.target = cueballOptions.maximum;
    }

    /*
     * DNS configuration: The delay and maxDelay values used in the previous
     * implementation were historically hardcoded to the same values that we use
     * use as defaults now.
     */
    r = cueballOptions.recovery.dns = {
        'retries': dflClientConnectRetries,
        'timeout': dflClientDnsTimeout,
        'delay': dflClientDnsDelayMin,
        'maxDelay': dflClientDnsDelayMax
    };

    if (options.dns) {
        if (Array.isArray(options.dns.resolvers)) {
            assert.arrayOfString(options.dns.resolvers,
                'options.dns.resolvers');
            cueballOptions.resolvers = options.dns.resolvers.slice(0);
        }

        if (options.dns.timeout) {
            assert.number(options.dns.timeout, 'options.dns.timeout');
            assert.ok(options.dns.timeout >= 0,
                'dns timeout must be non-negative');
            r.timeout = options.dns.timeout;
        }
    }

    /*
     * Right or wrong, the legacy behavior was that the timeout for each
     * request never increased.
     */
    r.maxTimeout = r.timeout;

    /*
     * DNS SRV configuration: SRV should fail fast, since it's not widely
     * deployed yet.
     */
    cueballOptions.recovery.dns_srv = jsprim.deepCopy(
        cueballOptions.recovery.dns);
    cueballOptions.recovery.dns_srv.retries = 0;

    /*
     * Default recovery configuration: we specify a 'default' recovery in
     * the cueball options that will cover both the initial connect attempt
     * and subsequent connect attempts.
     */
    r = cueballOptions.recovery.default = {};
    if (typeof (options.connectTimeout) == 'number') {
        assert.ok(options.connectTimeout >= 0,
            'connect timeout must be non-negative');
        r.timeout = options.connectTimeout;
    } else {
        r.timeout = dflClientConnectTimeout;
    }

    /*
     * As with DNS requests, connection operations historically used a fixed
     * timeout value.
     */
    r.maxTimeout = r.timeout;

    if (options.retry) {
        assert.optionalNumber(options.retry.retries, 'options.retry.retries');
        if (typeof (options.retry.retries) == 'number') {
            r.retries = options.retry.retries;
        } else {
            r.retries = dflClientConnectRetries;
        }

        /*
         * It's confusing, but the "timeout" for a retry policy is
         * really a delay.
         */
        assert.optionalNumber(options.retry.minTimeout,
            'options.retry.minTimeout');
        if (typeof (options.retry.minTimeout) == 'number') {
            r.delay = options.retry.minTimeout;

            if (typeof (options.retry.maxTimeout) == 'number') {
                assert.ok(options.retry.maxTimeout >=
                    options.retry.minTimeout,
                    'retry.maxTimeout must not be smaller ' +
                    'than retry.minTimeout');
                r.maxDelay = options.retry.maxTimeout;
            } else {
                r.delay = options.retry.minTimeout;
                r.maxDelay = Math.max(r.delay, dflClientDelayMax);
            }
        } else if (typeof (options.retry.maxTimeout) == 'number') {
            r.maxDelay = options.retry.maxTimeout;
            r.delay = Math.min(dflClientDelayMin, r.maxDelay);
        } else {
            r.delay = dflClientDelayMin;
            r.maxDelay = dflClientDelayMax;
        }

        assert.number(r.delay);
        assert.number(r.maxDelay);
        assert.ok(r.delay <= r.maxDelay);
    } else {
        r.retries = 0;
        r.delay = 0;
        r.maxDelay = r.delay;
    }

    return (cueballOptions);
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
 * The following named arguments must be specified:
 *
 *     log             bunyan-style logger
 *
 * You must also specify either:
 *
 *     cueballOptions  An object containing node-cueball configuration
 *                     parameters.  See the node-cueball documentation for
 *                     details on what these mean.  The MorayClient supports the
 *                     following properties:
 *
 *                         node-cueball Resolver properties: "domain",
 *                         "service", "defaultPort", "resolvers",
 *                         "maxDNSConcurrency"
 *
 *                         node-cueball ConnectionSet properties: "target",
 *                         "maximum"
 *
 *                         properties used by both Resolver and ConnectionSet:
 *                         "recovery"
 *
 *                     Other cueball parameters (like "log", "resolver",
 *                     "constructor") are supplied by the Moray client and may
 *                     not be specified here.
 *
 * or some combination of legacy options documented with
 * translateLegacyOptions() above.  It's strongly recommended that new consumers
 * use the "cueballOptions" approach because it's much less confusing and allows
 * specifying additional important parameters.
 *
 * You may also specify:
 *
 *     failFast         If true, this client emits "error" when the underlying
 *                      Cueball set reaches state "failed".  This is intended
 *                      for use by command-line tools to abort when it looks
 *                      like dependent servers are down.  Servers should
 *                      generally wait indefinitely for dependent services to
 *                      come up.
 *
 *     unwrapErrors     If false (the default), Errors emitted by this client
 *                      and RPC requests will contain a cause chain that
 *                      explains precisely what happened.  For example, if an
 *                      RPC fails with SomeError, you'll get back a
 *                      FastRequestError (indicating a request failure) caused
 *                      by a FastServerError (indicating that the failure was on
 *                      the remote server, as opposed to a local or
 *                      transport-level failure) caused by a SomeError.  In this
 *                      mode, you should use VError.findCause(err, 'SomeError')
 *                      to determine whether the root cause was a SomeError.
 *
 *                      If the "unwrapErrors" option is true, then Fast-level
 *                      errors are unwrapped and the first non-Fast error in the
 *                      cause chain is returned.  This is provided primarily for
 *                      compatibility with legacy code that uses err.name to
 *                      determine what kind of Error was returned.  New code
 *                      should prefer VError.findCause() instead.
 *
 *    mustCloseBeforeNormalProcessExit
 *
 *                      If true, then cause the program to crash if it would
 *                      otherwise exit 0 and this client has not been closed.
 *                      This is useful for making sure that client consumers
 *                      clean up after themselves.
 *
 * A sample invocation:
 *
 *     var client = moray.createClient({
 *         'log': bunyan.createLogger({
 *             'name': 'MorayClient',
 *             'level': process.env.LOG_LEVEL || 'debug',
 *             'stream': process.stdout
 *         }),
 *         'cueballOptions': {
 *             'domain': 'moray.mydatacenter.joyent.us',
 *             'maxDNSConcurrency': 3,
 *             'defaultPort': 2020,
 *             'target': 6,
 *             'maximum': 15,
 *             'recovery': {
 *                 'default': {
 *                     'retries': 5,
 *                     'timeout': 2000,
 *                     'maxTimeout': 10000,
 *                     'delay': 1000,
 *                     'maxDelay': 60000
 *                 }
 *             }
 *         }
 *     });
 */
function MorayClient(options) {
    var self = this;
    var cueballOptions;
    var resolver;

    EventEmitter.call(this);

    assert.object(options, 'options');
    assert.object(options.log, 'options.log');
    assert.optionalBool(options.unwrapErrors, 'options.unwrapErrors');
    assert.optionalBool(options.failFast, 'options.failFast');

    /*
     * Many of the client options determine how we configure the cueball module.
     * For compatibility with pre-cueball clients, we accept the old options and
     * translate them into arguments for cueball.  Modern clients may specify
     * cueball options directly, in which case we demand that they have not
     * specified any of these legacy options.
     */
    if (options.hasOwnProperty('cueballOptions')) {
        assert.ok(!options.hasOwnProperty('host'),
            'cannot combine "cueballOptions" with "host"');
        assert.ok(!options.hasOwnProperty('port'),
            'cannot combine "cueballOptions" with "port"');
        assert.ok(!options.hasOwnProperty('connectTimeout'),
            'cannot combine "cueballOptions" with "connectTimeout"');
        assert.ok(!options.hasOwnProperty('dns'),
            'cannot combine "cueballOptions" with "dns"');
        assert.ok(!options.hasOwnProperty('maxConnections'),
            'cannot combine "cueballOptions" with "maxConnections"');
        assert.ok(!options.hasOwnProperty('retry'),
            'cannot combine "cueballOptions" with "retry"');
        assert.string(options.cueballOptions.domain,
            'options.cueballOptions.domain');
        cueballOptions = jsprim.deepCopy(options.cueballOptions);
    } else {
        cueballOptions = translateLegacyOptions(options);
    }

    assert.string(cueballOptions.domain, 'cueballOptions.domain');

    /* Read-only metadata used for toString() and the like. */
    this.hostLabel = cueballOptions.domain;
    this.unwrapErrors = options.unwrapErrors ? true : false;
    this.failFast = options.failFast ? true : false;

    /* Helper objects. */
    this.log = options.log.child({
        component: 'MorayClient',
        domain: cueballOptions.domain
    }, true);

    this.log.debug(cueballOptions, 'init');

    resolver = cueball.resolverForIpOrDomain({
        'input': cueballOptions.domain + ':' + cueballOptions.defaultPort,
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
        'maximum': cueballOptions.maximum
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

    this.cueball.stop();
    this.cueballResolver.stop();
    this.log.info('closed');
    this.emit('close');
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
 *        emits 'end' or 'error'.
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

    function onEmitterRpcComplete() {
        assert.ok(!done);
        done = true;
        self.ctxRelease(rpcctx);
    }

    emitter.on('error', onEmitterRpcComplete);
    emitter.on('end', onEmitterRpcComplete);
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
MorayClient.prototype.createBucket = function createBucket(b, cfg, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.createBucket(rpcctx, b, cfg, opts,
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
MorayClient.prototype.getBucket = function getBucket(b, opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.getBucket(rpcctx, b, opts, this.makeReleaseCb(rpcctx, cb));
};


/**
 * Lists all buckets
 *
 * See https://mo.joyent.com/docs/moray/master/#ListBucket for more info.
 *
 * @param {Object} opts - request parameters
 * @param {Function} cb - callback
 */
MorayClient.prototype.listBuckets = function listBuckets(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }

    var rpcctx = this.ctxCreateForCallback(cb);

    if (rpcctx)
        buckets.listBuckets(rpcctx, opts, this.makeReleaseCb(rpcctx, cb));
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
MorayClient.prototype.deleteBucket = function deleteBucket(b, opts, cb) {
    assert.string(b, 'bucket');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.deleteBucket(rpcctx, b, opts, this.makeReleaseCb(rpcctx, cb));
};
MorayClient.prototype.delBucket = MorayClient.prototype.deleteBucket;


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
MorayClient.prototype.putObject = function putObject(b, k, v, opts, cb) {
    assert.string(b, 'bucket');
    assert.string(k, 'key');
    assert.object(v, 'value');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        objects.putObject(rpcctx, b, k, v, opts,
            this.makeReleaseCb(rpcctx, cb));
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
MorayClient.prototype.getObject = function getObject(b, k, opts, cb) {
    assert.string(b, 'bucket');
    assert.string(k, 'key');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        objects.getObject(rpcctx, b, k, opts, this.makeReleaseCb(rpcctx, cb));
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
MorayClient.prototype.deleteObject = function deleteObject(b, k, opts, cb) {
    assert.string(b, 'bucket');
    assert.string(k, 'key');
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        objects.deleteObject(rpcctx, b, k, opts,
            this.makeReleaseCb(rpcctx, cb));
};
MorayClient.prototype.delObject = MorayClient.prototype.deleteObject;


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
    assert.optionalObject(opts, 'options');

    var rpcctx = this.ctxCreateForEmitter();
    if (rpcctx) {
        var rv = objects.findObjects(rpcctx, b, f, (opts || {}));
        this.releaseWhenDone(rpcctx, rv);
        return (rv);
    }
    return (emitUnavailable());
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
 * Expose translateLegacyOptions privately for testing, not for the outside
 * world.
 */
MorayClient.privateTranslateLegacyOptions = translateLegacyOptions;

module.exports = {
    Client: MorayClient
};
