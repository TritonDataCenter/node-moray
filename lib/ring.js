// Copyright (c) 2014 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var assert = require('assert-plus');



///--API
function Ring(opts) {
    opts = opts || {};
    assert.object(opts);

    EventEmitter.call(this);

    var self = this;
    this._members = {};
    this._active = [];
    this._next = 0;
    this.priorityFunc = opts.priorityFunc;

    this.__defineGetter__('members', function () {
        return (Object.keys(self._members));
    });
    this.__defineGetter__('active', function () {
        return (this._active.slice(0));
    });
}
util.inherits(Ring, EventEmitter);
module.exports = Ring;

Ring.prototype.add = function add(key, obj) {
    assert.string(key);
    if (this.contains(key)) {
        throw new Error(util.format('key already exists: %s', key));
    }
    this._members[key] = obj;
};

Ring.prototype.remove = function remove(key) {
    this.contains(key, true);
    this.deactivate(key);
    delete this._members[key];
};

Ring.prototype.get = function get(key) {
    this.contains(key, true);
    return (this._members[key]);
};

Ring.prototype.contains = function contains(key, throwOnAbsent) {
    var present = (this._members[key] !== undefined);
    if (!present && throwOnAbsent) {
        throw new Error(util.format('invalid key: %s', key));
    }

    return (present);
};

Ring.prototype.activate = function activate(key, suppress) {
    if (this.isActive(key)) {
        return;
    }
    this.contains(key, true);
    this._active.push(key);
    this._active.sort();
    if (this._active.length === 1) {
        this.emit('online');
    }
    if (!suppress) {
        this.emit('deactivate', key);
    }
};

Ring.prototype.deactivate = function deactivate(key, suppress) {
    this.contains(key, true);
    var idx = this._active.indexOf(key);
    if (idx === -1) {
        return;
    }
    delete this._active[idx];
    // pull the null value off the array
    this._active.sort().pop();
    if (this._active.length === 0) {
        this.emit('offline');
    }
    if (!suppress) {
        this.emit('deactivate', key);
    }
};

Ring.prototype.next = function next() {
    if (this._active.length === 0) {
        return (null);
    }
    if (this._next >= this._active.length) {
        this._next = 0;
    }
    var key = this._active[this._next++];
    if (this.priorityFunc) {
        // Potentially override round-robin if priority function is present.
        // The entry with the _lowest_ score will be chosen.
        var self = this;
        var base = this.priorityFunc(this._members[key]);
        this._active.forEach(function (k) {
            var score = self.priorityFunc(self._members[k]);
            if (score < base) {
                base = score;
                key = k;
            }
        });
    }
    return (this._members[key]);
};

Ring.prototype.isActive = function isActive(key) {
    return (this._active.indexOf(key) !== -1);
};

Ring.prototype.isOnline = function isOnline() {
    return (this._active.length !== 0);
};
