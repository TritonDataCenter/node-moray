/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/client_params.js: normalize the various constructor parameters that the
 * Moray client supports.
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var net = require('net');
var url = require('url');

/*
 * Default client parameters
 */

var dflClientTargetConnections = 6;
var dflClientMaxConnections = 15;
var dflClientMaxDnsConcurrency = 3;
var dflClientConnectRetries = 5;
var dflClientConnectTimeout = 2000;     /* milliseconds */
var dflClientConnectTimeoutMax = 30000; /* milliseconds */
var dflClientDnsTimeout = 1000;         /* milliseconds */
var dflClientDnsTimeoutMax = 20000;     /* milliseconds */
var dflClientDnsDelayMin = 10;          /* milliseconds */
var dflClientDnsDelayMax = 10000;       /* milliseconds */
var dflClientDelayMin = 1000;           /* milliseconds */
var dflClientDelayMax = 60000;          /* milliseconds */

/* "service" associated with Moray DNS SRV records */
var dflClientCueballService = '_moray._tcp';
/* default TCP port for Moray servers */
var dflClientCueballDefaultPort = 2020;

/*
 * Note that correct handling of unspecified legacy options relies on the fact
 * that the defaults here match the defaults in legacy clients.
 */
var dflCueballOptions = {
    'service': dflClientCueballService,
    'defaultPort': dflClientCueballDefaultPort,
    'maxDNSConcurrency': dflClientMaxDnsConcurrency,
    'target': dflClientTargetConnections,
    'maximum': dflClientMaxConnections,
    'recovery': {
        /*
         * The 'default' recovery option will cover both the initial connect
         * attempt and subsequent connect attempts.
         */
        'default': {
            'retries': dflClientConnectRetries,
            'timeout': dflClientConnectTimeout,
            'maxTimeout': dflClientConnectTimeoutMax,
            'delay': dflClientDelayMin,
            'maxDelay': dflClientDelayMax
        },
        'dns': {
            'retries': dflClientConnectRetries,
            'timeout': dflClientDnsTimeout,
            'maxTimeout': dflClientDnsTimeoutMax,
            'delay': dflClientDnsDelayMin,
            'maxDelay': dflClientDnsDelayMax
        },
        /*
         * DNS SRV requests fail fast by default because it's not deployed
         * everywhere yet.
         */
        'dns_srv': {
            'retries': 0,
            'timeout': dflClientDnsTimeout,
            'maxTimeout': dflClientDnsTimeoutMax,
            'delay': dflClientDnsDelayMin,
            'maxDelay': dflClientDnsDelayMax
        }
    }
};

/*
 * This function takes the user-specified MorayClient parameters, figures out
 * what kind of connection the user wants, and returns a description back to the
 * caller that includes the cueballOptions that will be used for creating a
 * node-cueball Resolver and ConnectionSet.  This process is surprisingly
 * complicated, in part because we support a bunch of confusing legacy options,
 * but also because we need to support both SRV mode (where IP addresses and
 * ports are looked up in DNS using SRV records) and a direct mode (where the
 * user either specifies an IP address or -- and this is what makes it confusing
 * -- a DNS domain whose A records they want us to use to locate a backend).
 * See RFD 73 for the design background.
 *
 * This function accepts one of the following sets of options:
 *
 *    "srvDomain" (for SRV-based discovery)
 *
 *       OR
 *
 *    "url" (for A-based discovery or direct IP/port)
 *
 *       OR
 *
 *    "host" and optionally "port" (for A-based discovery or direct IP/port)
 *
 * along with one of the following sets of options:
 *
 *    "cueballOptions"  used to specify options related to the cueball Resolver
 *    			or ConnectionSet.  Supported cueball-related properties
 *    			include:
 *
 *                        o Resolver properties: "service", "defaultPort",
 *                          "resolvers", and "maxDNSConcurrency"
 *
 *                        o ConnectionSet properties: "target", "maximum"
 *
 *                        o common properties: "recovery"
 *
 *                      Other cueball parameters like "log", "resolver",
 *                      "constructor", and "domain" are supplied by the Moray
 *                      client and may not be specified here.
 *
 *       OR
 *
 *    any of the options documented under populateLegacyOptions() below.
 *
 * The return value is an object describing the configuration, including a set
 * of options used to create the Cueball Resolver and ConnectionSets.
 * Properties of the returned object are:
 *
 *     mode             either "srv" (for SRV-based discovery) or "direct" (for
 *                      specific IP/port connections).  This is mostly useful
 *                      for automated testing.
 *
 *     label            human-readable label for this configuration.  This is
 *                      something like the hostname or hostname and port.  It's
 *                      intended for log entries and the like.
 *
 *     cueballOptions   object specifying options needed to create the Cueball
 *                      Resolver and ConnectionSet.
 */
function parseMorayParameters(args) {
    var cueballopts, uoptions, port;
    var havetarget, havemax, u;

    /*
     * Some combination of "srvDomain", "url", or "host" (with optional "port")
     * must be specified.  We check the types here, and we'll check for the
     * presence of various combinations below.
     */
    assert.object(args, 'args');
    assert.optionalString(args.srvDomain, 'args.srvDomain');
    assert.optionalString(args.url, 'args.url');
    assert.optionalString(args.host, 'args.host');
    assert.optionalObject(args.cueballOptions, 'args.cueballOptions');

    /*
     * For legacy reasons, "port" may be a string or number.
     */
    assert.ok(typeof (args.port) == 'undefined' ||
        typeof (args.port) == 'string' ||
        typeof (args.port) == 'number',
        'args.port must be a string or number');

    /*
     * Process cueball options first, since that's common to each form of input.
     * We'll start with our default set of cueball options and override whatever
     * the user asked for.
     */
    cueballopts = jsprim.deepCopy(dflCueballOptions);
    uoptions = args.cueballOptions;
    if (typeof (uoptions) == 'object' && uoptions !== null) {
        /*
         * It's not allowed to specify cueballOptions with any of the legacy
         * options that we still support.
         */
        assert.ok(typeof (args.connectTimeout) == 'undefined',
            'cannot combine "cueballOptions" with "connectTimeout"');
        assert.ok(typeof (args.dns) == 'undefined',
            'cannot combine "cueballOptions" with "dns"');
        assert.ok(typeof (args.maxConnections) == 'undefined',
            'cannot combine "cueballOptions" with "maxConnections"');
        assert.ok(typeof (args.retry) == 'undefined',
            'cannot combine "cueballOptions" with "retry"');

        /*
         * It's not allowed to specify "domain" in cueballOptions.  You're
         * supposed to provide that information via one of the above options.
         */
        assert.equal(typeof (uoptions.domain), 'undefined',
            '"domain" may not be specified in cueballOptions');

        /*
         * For "defaultPort", "service", "maxDNSConcurrency", and "resolvers",
         * we'll just take whatever the user provided.
         */
        assert.optionalNumber(uoptions.defaultPort,
            'args.cueballOptions.defaultPort');
        if (typeof (uoptions.defaultPort) == 'number') {
            cueballopts.defaultPort = uoptions.defaultPort;
        }

        assert.optionalString(uoptions.service, 'args.cueballOptions.service');
        if (typeof (uoptions.service) == 'string') {
            cueballopts.service = uoptions.service;
        }

        assert.optionalNumber(uoptions.maxDNSConcurrency,
            'args.cueballOptions.maxDNSConcurrency');
        if (typeof (uoptions.maxDNSConcurrency) == 'number') {
            cueballopts.maxDNSConcurrency = uoptions.maxDNSConcurrency;
        }

        assert.optionalArrayOfString(uoptions.resolvers,
            'args.cueballOptions.resolvers');
        if (typeof (uoptions.resolvers) == 'object' &&
            uoptions.resolvers !== null) {
            cueballopts.resolvers = uoptions.resolvers;
        }

        /*
         * For "target" and "maximum", we demand that if the user specify one,
         * then they ought to specify both.  We could relax this and say that if
         * only one is specified, we'll pick a default for the other, but at
         * this point it seems clearer to require the consumer to specify both
         * (and it seems no harder for the them, either).
         */
        assert.optionalNumber(uoptions.target, 'args.cueballOptions.target');
        assert.optionalNumber(uoptions.maximum, 'args.cueballOptions.maximum');
        havetarget = typeof (uoptions.target) == 'number';
        havemax = typeof (uoptions.maximum) == 'number';
        if ((havetarget && !havemax) || (!havetarget && havemax)) {
            throw (new Error(
                'must specify neither or both of "target" and "maximum"'));
        }

        if (havetarget) {
            cueballopts.target = uoptions.target;
            cueballopts.maximum = uoptions.maximum;
        }

        /*
         * Similarly, if the caller specified "recovery" at all, we will assume
         * they completely specified what they want and we will not mess with
         * it.
         */
        assert.optionalObject(uoptions.recovery,
            'args.cueballOptions.recovery');
        if (typeof (uoptions.recovery) == 'object' &&
            uoptions.recovery !== null) {
            cueballopts.recovery = jsprim.deepCopy(uoptions.recovery);
        }
    } else {
        /*
         * Many of the client options determine how we configure the cueball
         * module.  For compatibility with pre-cueball clients, we accept the
         * old options and translate them into arguments for cueball.  Modern
         * clients may specify cueball options directly, in which case we demand
         * that they have not specified any of these legacy options.
         */
        populateLegacyOptions(cueballopts, args);
    }

    /*
     * Now that we've got the easy properties out of the way, pick apart the
     * options used to configure "domain" and "service".
     */
    if (typeof (args.srvDomain) == 'string') {
        assert.ok(!net.isIP(args.srvDomain),
            'cannot use "srvDomain" with an IP address');
        assert.notEqual(typeof (args.host), 'string',
            'cannot specify "host" with "srvDomain"');
        assert.equal(typeof (args.port), 'undefined',
            'cannot specify "port" with "srvDomain"');
        assert.notEqual(typeof (args.url), 'string',
            'cannot specify "url" with "srvDomain"');

        cueballopts.domain = args.srvDomain;
        return ({
            'mode': 'srv',
            'cueballOptions': cueballopts,
            'label': cueballopts.domain
        });
    }

    /*
     * If the user didn't specify "srvDomain", then they must have specified a
     * hostname or IP address and a port (possibly falling back to the default
     * port).  In this case, we connect directly to the specified IP (or one of
     * IPs specified by A records associated with the hostname).  We don't want
     * cueball to use SRV records in this mode.  There's not currently a way to
     * disable this, so we set a bogus service name and a very short timeout.
     */
    cueballopts.service = '_moraybogus._tcp';
    assert.strictEqual(cueballopts.recovery.dns_srv.retries, 0);
    cueballopts.recovery.dns_srv.timeout = 1;
    cueballopts.recovery.dns_srv.maxTimeout = 1;

    /*
     * This logic mirrors the legacy behavior of createClient, however
     * unnecessarily complicated that was.  Specifically, the desired host comes
     * from "args.host" if present, and otherwise from parsing "args.url".  The
     * desired port comes from "args.port" (as either a string or number) if
     * present, and otherwise the URL as long as "host" wasn't also specified,
     * and otherwise the default port 2020.
     */
    if (typeof (args.host) == 'string') {
        assert.notEqual(typeof (args.url), 'string',
            'cannot specify "host" with "url"');
        cueballopts.domain = args.host;
    } else {
        /* We've already checked this condition early on. */
        assert.strictEqual(typeof (args.url), 'string',
            'at least one of "srvDomain", "url", and "host" must be specified');
        u = url.parse(args.url);
        cueballopts.domain = u.hostname;
        if (u.port !== null) {
            port = u.port;
        }
    }

    if (typeof (args.port) == 'number' || typeof (args.port) == 'string') {
        port = args.port;
    }

    if (typeof (port) == 'number') {
        cueballopts.defaultPort = port;
    } else if (typeof (port) == 'string') {
        cueballopts.defaultPort = parseInt(port, 10);
        assert.ok(!isNaN(cueballopts.defaultPort), '"port" must be a number');
    }

    return ({
        'mode': 'direct',
        'cueballOptions': cueballopts,
        'label': cueballopts.domain + ':' + cueballopts.defaultPort
    });
}

/*
 * Given an assembled set of cueball options "out" (populated with our default
 * values) and a set of legacy options "args", process the legacy options and
 * update the cueball options.
 *
 * The following legacy properties are supported:
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
function populateLegacyOptions(out, args) {
    var r;

    assert.object(out, 'out');
    assert.object(args, 'args');

    assert.optionalNumber(args.maxConnections, 'args.maxConnections');
    if (typeof (args.maxConnections) == 'number') {
        out.maximum = args.maxConnections;
        out.target = Math.min(out.target, out.maximum);
    }

    assert.optionalObject(args.dns, 'args.dns');
    if (args.dns) {
        assert.optionalArrayOfString(args.dns.resolvers,
            'args.dns.resolvers');
        if (Array.isArray(args.dns.resolvers)) {
            out.resolvers = args.dns.resolvers.slice(0);
        }
    }

    r = out.recovery;
    if (args.dns && typeof (args.dns.timeout) == 'number') {
        assert.number(args.dns.timeout, 'args.dns.timeout');
        assert.ok(args.dns.timeout >= 0, 'dns timeout must be non-negative');
        r.dns.timeout = args.dns.timeout;
        r.dns_srv.timeout = args.dns.timeout;

        /*
         * In the old implementation, the timeout never increased.  Based on
         * experience, that's not reasonable default behavior, so we'll try to
         * use our default maximum unless that's too small.
         */
        if (r.dns.maxTimeout < r.dns.timeout) {
            r.dns.maxTimeout = r.dns.timeout;
            r.dns_srv.maxTimeout = r.dns.maxTimeout;
        }
    }

    assert.optionalNumber(args.connectTimeout, 'args.connectTimeout');
    if (typeof (args.connectTimeout) == 'number') {
        assert.ok(args.connectTimeout >= 0,
            'connect timeout must be non-negative');
        r.default.timeout = args.connectTimeout;
        if (r.default.maxTimeout < r.default.timeout) {
            r.default.maxTimeout = r.default.timeout;
        }
    }

    assert.optionalObject(args.retry, 'args.retry');
    if (args.retry) {
        assert.optionalNumber(args.retry.retries, 'args.retry.retries');
        if (typeof (args.retry.retries) == 'number') {
            r.default.retries = args.retry.retries;
        }

        /*
         * In the legacy interface, the retry policy is specified in terms of
         * timeouts.  Those timeout values really describe the delay between
         * attempts, which we now call "delay".
         */
        assert.optionalNumber(args.retry.minTimeout,
            'args.retry.minTimeout');
        if (typeof (args.retry.minTimeout) == 'number') {
            r.default.delay = args.retry.minTimeout;

            if (typeof (args.retry.maxTimeout) == 'number') {
                assert.ok(args.retry.maxTimeout >=
                    args.retry.minTimeout,
                    'retry.maxTimeout must not be smaller ' +
                    'than retry.minTimeout');
                r.default.maxDelay = args.retry.maxTimeout;
            } else {
                r.default.delay = args.retry.minTimeout;
                if (r.default.maxDelay < r.default.delay) {
                    r.default.maxDelay = r.default.delay;
                }
            }
        } else if (typeof (args.retry.maxTimeout) == 'number') {
            r.default.maxDelay = args.retry.maxTimeout;
            if (r.default.delay > r.default.maxDelay) {
                r.default.delay = r.default.maxDelay;
            }
        }

        assert.number(r.default.delay);
        assert.number(r.default.maxDelay);
        assert.ok(r.default.delay <= r.default.maxDelay);
    }

    if (args.failFast) {
        r.default.retries = 0;
        r.default.delay = 0;
        r.default.maxDelay = 0;
    }
}

exports.parseMorayParameters = parseMorayParameters;
