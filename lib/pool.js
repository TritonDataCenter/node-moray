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
 *
 *
 * Connection state machine
 *
 * Connections are provided to us by Cueball once they've been successfully
 * established.  We use them for new requests until Cueball removes them,
 * which typically happens because a particular backend is no longer listed as
 * in-service in DNS or because Cueball has shuffled its set of connected
 * backends.  We have found in some cases that the underlying mechanism flaps
 * between two disjoint sets of connection (e.g., a set driven by SRV records
 * and one driven by A records, with flaps between the two resulting from
 * transient DNS issues).  To mitigate the impact of this, if we would be
 * removing our last connection, we instead hold onto it for a brief period.
 *
 *           |
 *           | cueball: 'added'
 *           | connAdd()
 *           v
 *  +---------------------+
 *  | state: MC_S_AVAIL   | ------------------------+ cueball: "removed",
 *  +---------------------+                         | on last connection
 *           |                                      v connRetire()
 *           | cueball: "removed",      +----------------------+
 *           | not last connection      | state: MC_S_FALLBACK |
 *           | connRetire()/connDrain() +----------------------+
 *           v                                      |
 *  +---------------------+                         | fallback time expires
 *  | state: MC_S_DRAIN   | <-----------------------+ or another connection is
 *  +---------------------+                           added
 *           |                                        connFallbackRemove()
 *           | last request completes
 *           | connDelete()
 *           v
 *  +---------------------+
 *  | state: MC_S_DELETED |
 *  |    (removed)        |
 *  +---------------------+
 */

var assert = require('assert-plus');
var jsprim = require('jsprim');
var upq = require('updatablepriorityqueue');
var VError = require('verror');

/*
 * Maximum time (in milliseconds) that we will continue to use a connection when
 * it was the last connection that we knew about.
 */
var MorayFallbackMaxTime = 15 * 1000;

/*
 * Connection states (see above)
 */
var MC_S_AVAIL = '_moray_conn_state_avail';
var MC_S_DRAIN = '_moray_conn_state_drain';
var MC_S_FALLBACK = '_moray_conn_state_fallback';
var MC_S_DELETED = '_moray_conn_state_deleted';

module.exports = MorayConnectionPool;

/*
 * This is a struct-like class that represents a single logical connection.  The
 * lifecycle and logic are managed by the MorayConnectionPool class.
 */
function MorayConnection(key, conn, hdl, log) {
    assert.string(key, 'key');
    assert.object(conn, 'conn');
    assert.object(hdl, 'hdl');
    assert.object(log, 'log');
    this.mc_key = key;      /* cueball identifier for this connection */
    this.mc_conn = conn;    /* object implementing Cueball "Connection" */
    this.mc_hdl = hdl;      /* cueball handle to release after drain */
    this.mc_log = log;      /* bunyan-style logger */
    this.mc_nreqs = 0;      /* number of outstanding requests */
    this.mc_state = MC_S_AVAIL;
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
     * We keep track of all connections that we know about in "mcp_conns".  This
     * includes connections available for general use for new requests,
     * connections that have been removed from service that still have pending
     * requests (because they're no longer reported in service discovery or
     * because cueball is rebalancing its connection set), and at most one
     * fallback connection.  The keys in "mcp_conns" are the cueball-provided
     * connection keys, and the values are the connections themselves.
     *
     * We separately track in "mcp_avail" the set of connections that cueball
     * considers in-service.  Keys in "mcp_avail" are cueball-provided
     * connection keys (just as with "mcp_conns"), but the values are not
     * meaningful since it's just a set.  The keys actually present in
     * "mcp_avail" are always a subset of those in "mcp_conns".
     *
     * There may be at most one connection in "mcp_conn_fallback" that has been
     * removed from general use but remains available if we have no connections
     * formally in service.  See above for details on this mechanism.  This
     * connection also appears in "mcp_conns" (like all connections that we know
     * about).
     */
    this.mcp_conns = {};    /* all connections, by key */
    this.mcp_avail = {};    /* keys for conns cueball considers in-service */

    /* priority queue of available connections */
    this.mcp_pq = new upq(function (o) { return (o.k); },
                          function (o) { return (o.v); });

    /* Fallback connection information.  See above. */
    this.mcp_fallback_enable = true;    /* fallback behavior enabled */
    this.mcp_conn_fallback = null;      /* fallback connection itself */
    this.mcp_conn_fallback_time = null; /* hrtime when it was made fallback */

    /* Counters for debugging */
    this.mcp_nalloc_ok = 0;         /* successful allocations */
    this.mcp_nalloc_fail = 0;       /* failed allocations */
    this.mcp_nalloc_fallback = 0;   /* allocations of a fallback conn */
    this.mcp_nreleased = 0;         /* releases */
    this.mcp_nfallbacks = 0;        /* assigned a conn as fallback */

    this.mcp_cueball_set.on('added', function onConnectionAdd(key, conn, hdl) {
        self.connAdd(key, conn, hdl);
    });

    this.mcp_cueball_set.on('removed', function onConnectionRemoved(key) {
        self.connRetire(key);
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
    var entry, availkeys, key, mconn, aconn, staletime, staletimems;

    /*
     * Our release and drain functionality often requires access by key, so
     * maintaining a pure priority-queue is a little bit complex. We use
     * an "availability" hash along with a priority-queue. The availability
     * hash is authoritative (ie the priority queue may have entries which
     * are no longer available, so we have to check).
     *
     * Randomly picking Moray servers works when their response times are
     * all uniform; but small differences in response times, (especially
     * with bursty workloads) can end up assigning too much work to the
     * slowest server(s). In cases where the server performance is partly
     * dictated by the amount of work, we end up with a positive feedback
     * loop where the slowest servers acquire a mounting backlog while the
     * least-loaded stay lightly loaded.
     *
     * Get the next entry from our priority-queue, check that it is still
     * a valid entry.
     */
    availkeys = Object.keys(this.mcp_avail);
    if (availkeys.length > 0) {
        // Get the lowest-connection-count entry, and sanity check for validity.
        while ((entry = this.mcp_pq.poll()) !== undefined) {
            key = entry.k;
            /*
             * We got an entry from our queue, is it legal to use ?
             * NOTE: if it wasn't valid, it has now been removed.
             */
            if (this.mcp_avail.hasOwnProperty(key))
                break;
        }

        mconn = this.mcp_conns[key];
        assert.strictEqual(mconn.mc_state, MC_S_AVAIL);
    } else {
        mconn = this.mcp_conn_fallback;
        if (mconn !== null) {
            assert.arrayOfNumber(this.mcp_conn_fallback_time);
            assert.strictEqual(mconn.mc_state, MC_S_FALLBACK);
            key = mconn.mc_key;
            staletime = process.hrtime(this.mcp_conn_fallback_time);
            staletimems = jsprim.hrtimeMillisec(staletime);
            if (staletimems > MorayFallbackMaxTime) {
                mconn = null;
                this.connFallbackRemove({
                    'staleTimeMs': staletimems,
                    'maxTimeMs': MorayFallbackMaxTime,
                    'reason': 'fallback is too old'
                });
            } else {
                this.mcp_nalloc_fallback++;
            }
        }
    }

    if (mconn === null) {
        this.mcp_nalloc_fail++;
        this.mcp_log.trace('failed to allocate connection');
        return (new VError({
            'name': 'NoBackendsError'
        }, 'no connections available'));
    }

    assert.ok(mconn instanceof MorayConnection);
    assert.ok(mconn.mc_nreqs >= 0);
    mconn.mc_nreqs++;
    // Add back to our priority queue.
    this.mcp_pq.add({'k': mconn.mc_key, 'v': mconn.mc_nreqs});
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
    if (mconn.mc_state == MC_S_AVAIL) {
        assert.ok(this.mcp_avail.hasOwnProperty(key));
        // Update our priority-queue
        this.mcp_pq.updateElement(key, {'k': key, 'v': mconn.mc_nreqs});
    } else {
        assert.ok(!this.mcp_avail.hasOwnProperty(key));
        assert.ok(mconn.mc_state == MC_S_DRAIN ||
            mconn.mc_state == MC_S_FALLBACK);
        if (mconn.mc_state == MC_S_DRAIN && mconn.mc_nreqs === 0) {
            this.connDelete(key);
            // remove from the priority-queue, if present
            this.mcp_pq.deleteElement(key);
        }
    }
};

/*
 * [private] Invoked by cueball when a new connection has been established and
 * is ready for use.  Just add it to our set of available connections.
 */
MorayConnectionPool.prototype.connAdd = function connAdd(key, conn, hdl) {
    var mconn, extras;

    assert.ok(!this.mcp_conns.hasOwnProperty(key));
    assert.ok(!this.mcp_avail.hasOwnProperty(key));

    extras = conn.socketAddrs();
    extras.key = key;
    mconn = new MorayConnection(key, conn, hdl,
        this.mcp_log.child(extras, true));
    assert.strictEqual(mconn.mc_state, MC_S_AVAIL);
    this.mcp_conns[key] = mconn;
    this.mcp_avail[key] = true;
    // Also add to our min-priority-queue with tip-top priority (0).
    this.mcp_pq.add({ 'k': key, 'v': 0});
    mconn.mc_log.info('new connection');

    this.connFallbackRemove({ 'reason': 'new connection' });
};

/*
 * [private] Invoked when cueball determines that a connection should be removed
 * from service.  This connection may still be in use by any number of requests.
 * It's our responsibility to stop assigning new work to it, wait for existing
 * requests to complete, and close the connection.
 *
 * This function always removes the specified connection from general-purpose
 * use by new requests.  However, if this is the last connection we have, then
 * we hold onto it as a fallback and don't drain it right away.
 */
MorayConnectionPool.prototype.connRetire = function (key) {
    var mconn;

    assert.ok(this.mcp_conns.hasOwnProperty(key));
    assert.ok(this.mcp_avail.hasOwnProperty(key));
    mconn = this.mcp_conns[key];
    assert.strictEqual(mconn.mc_state, MC_S_AVAIL);

    /*
     * Remove the connection from service for new requests.
     * Also remove from our priority-queue.
     */
    delete (this.mcp_avail[key]);
    this.mcp_pq.deleteElement(key);

    assert.bool(mconn.mc_conn.destroyed);
    if (!jsprim.isEmpty(this.mcp_avail) || !this.mcp_fallback_enable ||
        mconn.mc_conn.destroyed) {
        this.connDrain(key);
    } else {
        /*
         * This was the last available connection.  There must not already be a
         * fallback because we would have removed that fallback when we added
         * this connection.
         */
        assert.strictEqual(this.mcp_conn_fallback, null);
        assert.strictEqual(this.mcp_conn_fallback_time, null);
        mconn.mc_state = MC_S_FALLBACK;
        mconn.mc_log.warn('retiring last connection (saving as fallback)');
        this.mcp_nfallbacks++;
        this.mcp_conn_fallback = mconn;
        this.mcp_conn_fallback_time = process.hrtime();
    }
};

/*
 * [private] Invoked as part of connection teardown when we're definitely
 * removing a connection from service.  This means either cueball has asked us
 * to remove it and we have other connections available, or this was a fallback
 * connection that we're no longer intending to use.
 */
MorayConnectionPool.prototype.connDrain = function connDrain(key) {
    var mconn;

    assert.ok(!this.mcp_avail.hasOwnProperty(key));
    assert.ok(this.mcp_conns.hasOwnProperty(key));

    /*
     * If there are no requests using this connection, clean it up now.  If
     * there are, wait for those to finish and clean up when they're done.
     */
    mconn = this.mcp_conns[key];
    assert.ok(mconn.mc_state == MC_S_AVAIL || mconn.mc_state == MC_S_FALLBACK);
    mconn.mc_state = MC_S_DRAIN;
    if (mconn.mc_nreqs === 0) {
        this.connDelete(key);
    } else {
        mconn.mc_log.info({ 'nreqs': mconn.mc_nreqs }, 'waiting for drain');
    }
};

/*
 * [private] Invoked when we know that a connection is fully quiesced (there are
 * no requests associated with it) to destroy it.
 */
MorayConnectionPool.prototype.connDelete = function (key) {
    var mconn;

    assert.ok(!this.mcp_avail.hasOwnProperty(key));
    assert.ok(this.mcp_conns.hasOwnProperty(key));

    mconn = this.mcp_conns[key];
    assert.strictEqual(mconn.mc_state, MC_S_DRAIN);
    assert.ok(this.mcp_conn_fallback != mconn,
        'attempted to delete fallback connection');
    assert.strictEqual(mconn.mc_nreqs, 0);
    delete (this.mcp_conns[key]);

    /*
     * By the time the following completes, there should be no references to
     * this object any more.  We mark the state DELETED in case we find it in
     * the debugger (and so that assertions can tell if we've accidentally held
     * onto something we shouldn't have).
     */
    mconn.mc_state = MC_S_DELETED;
    mconn.mc_log.info('removed connection');
    mconn.mc_hdl.release();
};

/*
 * [private] Removes and cleans up the fallback connection, if any.
 */
MorayConnectionPool.prototype.connFallbackRemove = function (props) {
    var mconn;

    assert.object(props, 'props');
    assert.string(props.reason, 'props.reason');

    if (this.mcp_conn_fallback === null) {
        assert.strictEqual(this.mcp_conn_fallback_time, null);
        return;
    }

    assert.notStrictEqual(this.mcp_conn_fallback_time, null);
    mconn = this.mcp_conn_fallback;
    this.mcp_conn_fallback = null;
    this.mcp_conn_fallback_time = null;

    assert.strictEqual(mconn.mc_state, MC_S_FALLBACK);
    mconn.mc_log.info(props, 'removing fallback connection');
    this.connDrain(mconn.mc_key);
};

/*
 * [private] Indicates that we should stop maintaining a fallback connection
 * (generally because the parent client is shutting down).
 */
MorayConnectionPool.prototype.fallbackDisable = function ()
{
    this.mcp_fallback_enable = false;
    this.connFallbackRemove({ 'reason': 'shutting down' });
};
