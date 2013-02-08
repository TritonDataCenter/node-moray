// Copyright (c) 2012 Joyent, Inc.  All rights reserved.

var EventEmitter = require('events').EventEmitter;
var util = require('util');

var assert = require('assert-plus');



///--- Globals

var sprintf = util.format;



///--- Internal Helpers

function increment(start, size, orig) {
        var _next = (start + 1) % size;

        if (_next === orig)
                return (false);

        return (_next);
}



///--API

function Ring(opts) {
        EventEmitter.call(this);

        if (typeof (opts) === 'object') {
                assert.number(opts.size, 'options.size');
        } else if (typeof (opts) === 'number') {
                opts = { size: opts };
        } else {
                throw new TypeError('size (number) required');
        }

        this.count = 0;
        this.iter = 0;
        this.start = 0;
        this.size = opts.size;
        this.ring = new Array(opts.size);

        var self = this;
        this.__defineGetter__('length', function () {
                return (self.ring.length);
        });
}
util.inherits(Ring, EventEmitter);
module.exports = Ring;


Ring.prototype.empty = function empty() {
        return (this.count === 0);
};


Ring.prototype.filter = function filter(f, thisp) {
        assert.func(f, 'callback');
        assert.optionalObject(thisp, 'this');

        var i = this.start;
        var res = [];

        do {
                var val = this.ring[i];
                if (f.call(thisp, val, i, this))
                        res.push(val);

                if (++i === this.ring.length)
                        i = 0;
        } while (i !== this.start);

        return (res);
};


Ring.prototype.full = function full() {
        return (this.count === this.size);
};


Ring.prototype.next = function next() {
        if (++this.iter >= this.ring.length)
                this.iter = 0;

        return (this.ring[this.iter]);
};


Ring.prototype.peek = function peek() {
        return (this.ring[this.start]);
};


Ring.prototype.pop = function pop() {
        var obj = this.ring[this.start];
        this.start = (this.start + 1) % this.size;
        this.count--;

        return (obj);
};


Ring.prototype.push = function push(obj) {
        if (obj === undefined)
                return (false);

        var end = (this.start + this.count) % this.size;
        this.ring[end] = obj;

        if (this.count === this.size) {
                this.start = (this.start + 1) % this.size;
        } else {
                this.count++;
        }

        return (this.count);
};


Ring.prototype.remove = function remove(obj) {
        if (obj === undefined)
                return (false);

        var ndx = this.ring.indexOf(obj);
        if (ndx === -1)
                return (false);

        this.ring = this.ring.splice(ndx, 1);
        this.start = 0; // just reset
        this.count--;

        return (this.count);
};


Ring.prototype.toString = function toString() {
        var s = sprintf('[object Ring <size=%d, count=%d>]',
                        this.size,
                        this.count);

        return (s);
};
