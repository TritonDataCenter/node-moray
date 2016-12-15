/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * lib/pool.js: Moray connection pool implementation
 *
 * The Moray client leverages the Cueball module for service discovery (via DNS)
 * and for managing TCP connections to a set of backends found via service
 * discovery.  Cueball is responsible for establishing connections (using
 * connection timeouts and backoff as appropriate) and gracefully cleaning up
 * after them when they fail.
 *
 * Cueball also provides a connection pool interface that's oriented around
 * protocols that only support one consumer at a time (like HTTP).  Our use-case
 * is quite different because we can multiplex a large number of requests over
 * the same TCP connection.  As a result, our policy for how many connections to
 * maintain to each instance, the way we allocate and track connections for each
 * request, and the way we react to failures is pretty different than what the
 * connection pool expects.  To accommodate that, we use the simpler
 * ConnectionSet interface, which just maintains a set of connections for us.
 * Here, we implement allocation and connection tracking appropriately.
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var VError = require('verror');

module.exports = MorayConnectionPool;

/*
 * This is a struct-like class that represents a single logical connection.  The
 * lifecycle and logic are managed by the MorayConnectionPool class.
 */
function MorayConnection(key, conn, log) {
    assert.string(key, 'key');
    assert.object(conn, 'conn');
    assert.object(log, 'log');
    this.mc_key = key;      /* cueball identifier for this connection */
    this.mc_conn = conn;    /* object implementing Cueball "Connection" */
    this.mc_log = log;      /* bunyan-style logger */
    this.mc_nreqs = 0;      /* number of outstanding requests */
}

/*
 * This is a struct-like class representing a single allocation of a
 * MorayConnection.  This primarily exists to allow us to ensure that consumers
 * release each connection exactly once.  Double-releases will result in a
 * thrown exception, and leaks will be at least somewhat debuggable.
 */
function MorayConnectionAllocation(mconn) {
    this.mca_mconn = mconn;
    this.mca_released = false;
}

MorayConnectionAllocation.prototype.connection = function () {
    return (this.mca_mconn.mc_conn);
};

/*
 * Given a Cueball ConnectionSet, implements a simple allocate/release interface
 * using the connections in that set.
 */
function MorayConnectionPool(args) {
    var self = this;

    assert.object(args, 'args');
    assert.object(args.log, 'args.log');
    assert.object(args.cueballResolver, 'args.cueballResolver');
    assert.object(args.cueballSet, 'args.cueballSet');

    this.mcp_log = args.log;
    this.mcp_cueball_resolver = args.cueballResolver;
    this.mcp_cueball_set = args.cueballSet;

    /*
     * We keep track of all connections that we know about, as well as the set
     * of connections that are available for new work to be assigned.  These
     * aren't the same, as there may be some connections that are being drained
     * because they've disappeared from service discovery or because cueball is
     * rebalancing the set.  Both of these sets are indexed by the
     * cueball-provided key for each connection.
     */
    this.mcp_conns = {};    /* all connections */
    this.mcp_avail = {};    /* connections in service for new requests */

    /* Counters for debugging */
    this.mcp_nalloc_ok = 0;     /* successful allocations */
    this.mcp_nalloc_fail = 0;   /* failed allocations */
    this.mcp_nreleased = 0;     /* releases */

    this.mcp_cueball_set.on('added', function onConnectionAdd(key, conn) {
        self.connAdd(key, conn);
    });

    this.mcp_cueball_set.on('removed', function onConnectionRemoved(key) {
        self.connDrain(key);
    });
}

/*
 * [public] Pick an available connection to use for a new request.  On success,
 * returns an object that the caller can use to make requests.  On failure,
 * returns an Error describing the problem.
 *
 * The caller must invoke connRelease() when the request is finished.
 */
MorayConnectionPool.prototype.connAlloc = function () {
    var availkeys, key, mconn, aconn;

    /*
     * There are more sophisticated ways to pick a connection (e.g., store
     * connections in a priority queue by number of outstanding requests).  But
     * our expectation is that Moray servers are pretty uniform, Moray requests
     * are pretty uniform in their cost on the server, and so a random
     * distribution is likely to be reasonable.
     */
    availkeys = Object.keys(this.mcp_avail);
    if (availkeys.length === 0) {
        this.mcp_nalloc_fail++;
        this.mcp_log.trace('failed to allocate connection');
        return (new VError({
            'name': 'NoBackendsError'
        }, 'no connections available'));
    }

    key = jsprim.randElt(availkeys);
    mconn = this.mcp_conns[key];
    assert.ok(mconn instanceof MorayConnection);
    assert.ok(mconn.mc_nreqs >= 0);
    mconn.mc_nreqs++;
    aconn = new MorayConnectionAllocation(mconn);
    mconn.mc_log.trace('allocated connection');
    this.mcp_nalloc_ok++;
    return (aconn);
};

/*
 * [public] Release a connection allocated from connAlloc().  The caller should
 * not do anything else with the connection.
 */
MorayConnectionPool.prototype.connRelease = function (aconn) {
    var mconn, key;

    assert.ok(aconn instanceof MorayConnectionAllocation);
    assert.ok(!aconn.mca_released, 'double-release of Moray connection');

    mconn = aconn.mca_mconn;
    assert.ok(mconn.mc_nreqs > 0);

    aconn.mca_released = true;
    mconn.mc_nreqs--;
    mconn.mc_log.trace({ 'nreqs': mconn.mc_nreqs }, 'released connection');
    this.mcp_nreleased++;

    key = mconn.mc_key;
    if (!this.mcp_avail.hasOwnProperty(key) && mconn.mc_nreqs === 0) {
        this.connDelete(key);
    }
};

/*
 * [private] Invoked by cueball when a new connection has been established and
 * is ready for use.  Just add it to our set of available connections.
 */
MorayConnectionPool.prototype.connAdd = function connAdd(key, conn) {
    var mconn, extras;

    assert.ok(!this.mcp_conns.hasOwnProperty(key));
    assert.ok(!this.mcp_avail.hasOwnProperty(key));

    extras = conn.socketAddrs();
    extras.key = key;
    mconn = new MorayConnection(key, conn, this.mcp_log.child(extras, true));
    this.mcp_conns[key] = mconn;
    this.mcp_avail[key] = true;
    mconn.mc_log.info('new connection');
};

/*
 * [private] Invoked by cueball when a connection should be removed from
 * service.  This connection may well still be in use by any number of requests.
 * It's our responsibility to stop assigning new work to it, wait for existing
 * requests to complete, and close the connection.
 */
MorayConnectionPool.prototype.connDrain = function connDrain(key) {
    var mconn;

    assert.ok(this.mcp_conns.hasOwnProperty(key));
    assert.ok(this.mcp_avail.hasOwnProperty(key));

    /*
     * Remove the connection from service for new requests.
     */
    delete (this.mcp_avail[key]);

    /*
     * If there are no requests using this connection, clean it up now.  If
     * there are, wait for those to finish and clean up when they're done.
     */
    mconn = this.mcp_conns[key];
    if (mconn.mc_nreqs === 0) {
        this.connDelete(key);
    } else {
        mconn.mc_log.info({ 'nreqs': mconn.mc_nreqs }, 'waiting for drain');
    }
};

/*
 * [private] Invoked when we know that a connection is fully quiesced (there are
 * no requests associated with it) to remove it from service and destroy it.
 */
MorayConnectionPool.prototype.connDelete = function (key) {
    var mconn;

    assert.ok(!this.mcp_avail.hasOwnProperty(key));
    assert.ok(this.mcp_conns.hasOwnProperty(key));

    mconn = this.mcp_conns[key];
    assert.strictEqual(mconn.mc_nreqs, 0);
    delete (this.mcp_conns[key]);

    mconn.mc_log.info('removed connection');
    mconn.mc_conn.destroy();
};
