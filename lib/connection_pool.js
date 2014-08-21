// Copyright (c) 2014, Joyent, Inc. All rights reserved.


var EventEmitter = require('events').EventEmitter;
var util = require('util');
var assert = require('assert-plus');
var once = require('once');
var fast = require('fast');


var Ring = require('./ring');

///--- Helpers

function leastRequestsPriority(conn) {
    return (conn.countPending);
}

///--- API

function ConnectionPool(opts) {
    EventEmitter.call(this);
    assert.object(opts, 'opts');
    assert.number(opts.connectTimeout, 'opts.connectTimeout');
    assert.number(opts.max);
    assert.object(opts.log);

    this.log = opts.log;
    this.port = opts.port;
    this.max = opts.max;
    this.connectTimeout = opts.connectTimeout;
    if (typeof (opts.retries) === 'number') {
        this.retry = {
            minTimeout: 1000,
            maxTimeout: 10000,
            retries: opts.retries
        };
    } else {
        this.retry = opts.retries || { retries: Infinity };
    }

    this.ring = new Ring();
    var self = this;
    var forwardEvents = ['online', 'offline', 'activate', 'deactivate'];
    forwardEvents.forEach(function (event) {
        self.ring.on(event, self.emit.bind(self, event));
    });

    this.__defineGetter__('connected', function () {
        return (self.ring.active.length !== 0);
    });
}
util.inherits(ConnectionPool, EventEmitter);
module.exports = ConnectionPool;

ConnectionPool.prototype.setHosts = function setHosts(ipAddrs) {
    assert.arrayOfString(ipAddrs);

    var self = this;
    var current = this.ring.members;
    var remove = current.filter(function (addr) {
        return (ipAddrs.indexOf(addr) === -1);
    });
    var add = ipAddrs.filter(function (addr) {
        return (current.indexOf(addr) === -1);
    });

    remove.forEach(function (addr) {
        self.removeHost(addr);
    });
    add.forEach(function (addr) {
        self.addHost(addr);
    });
};

ConnectionPool.prototype.addHost = function addHost(host) {
    if (this.closed) {
        return;
    }

    var self = this;
    var cRing = new Ring({
        priorityFunc: leastRequestsPriority
    });
    this.ring.add(host, cRing);
    cRing.on('online', function () {
        self.log.debug({host: host}, 'host online');
        self.ring.activate(host);
    });
    cRing.on('offline', function () {
        self.log.debug({host: host}, 'host offline');
        self.ring.deactivate(host);
    });

    // setup ring of connections
    for (var i = 0; i < this.max; i++) {
        var key = '' + i;
        // FIXME: tune defaults
        var client = fast.createClient({
            host: host,
            port: this.port,
            connectTimeout: this.connectTimeout,
            retry: this.retry,
            reconnect: true
        });
        client.log = this.log.child({
            fastClient: host + '-' + i
        });

        cRing.add(key, client);
        client.on('connect', cRing.activate.bind(cRing, key));
        client.on('close', cRing.deactivate.bind(cRing, key));
        client.on('error', this.emit.bind(this, 'error'));
    }
};

ConnectionPool.prototype.removeHost = function removeHost(host) {
    var cRing = this.ring.get(host);

    // silence events during teardown
    this.ring.deactivate(host, true);
    cRing.removeAllListeners();

    this.ring.remove(host);
    cRing.members.forEach(function (key) {
        var client = cRing.get(key);
        client.close();
    });
};

ConnectionPool.prototype.next = function next() {
    var client = null;
    var cRing = this.ring.next();
    if (cRing) {
        client = cRing.next();
    }
    return (client);
};

ConnectionPool.prototype.close = function close() {
    var self = this;
    if (!this.closed) {
        this.closed = true;
        this.ring.members.forEach(function (host) {
            self.removeHost(host);
        });
        this.emit('close');
    }
};
