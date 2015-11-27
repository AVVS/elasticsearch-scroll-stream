'use strict';

var Readable = require('readable-stream').Readable;
var Promise = require('bluebird');
var inherits = require('util').inherits;

function ESScrollStream(opts) {
    Readable.call(this, {
        objectMode: true,
        highWaterMark: opts.highWaterMark || 1000
    });

    var client = opts.client,
        query = opts.query;

    if (!client) {
        throw new Error('`opts.client` must be an elasticsearch client');
    }

    if (!query || !query.scroll) {
        throw new Error('`opts.query` must be a valid elasticsearch scroll initiation request');
    }

    this.client = client;
    this.query = query;
    this.scrollId = null;
    this.currentRequest = null;

    // attempts
    this.retry = opts.retry || 3;
    if (typeof opts.retryFunc === 'function') {
        this._retryAttempt = opts.retryFunc;
    }

    // buffer settings
    this.streamed = 0;
    this.lowWaterMark = opts.lowWaterMark || 1000;
}
inherits(ESScrollStream, Readable);

ESScrollStream.prototype._retryAttempt = function (attempt) {
    return Math.pow(attempt, 2) * 500;
};

ESScrollStream.prototype._read = function () {
    if (this.currentRequest || this._next === false) {
        this.pushRequested = true;
        return;
    }

    this.currentRequest = this._fetchPage().finally(function () {
        this.currentRequest = null;

        if (this._next !== false && (this.pushRequested || this._readableState.buffer.length < this.lowWaterMark)) {
            this.pushRequested = false;
            this._read();
        }
    });
};

ESScrollStream.prototype._fetchPage = function (attempt) {
    attempt = attempt || 0;

    // cache vars
    var scrollId = this.scrollId,
        client = this.client,
        query = this.query;

    // initial promise
    var promise;
    if (!scrollId) {
        promise = client.search(query).bind(this).then(function initialScrollRequest(resp) {
            this.emit('total', resp.hits.total);
            return resp;
        });
    } else {
        promise = client
            .scroll({ scrollId: scrollId, scroll: query.scroll })
            .bind(this)
            .catch(function (err) {
                if (++attempt < this.retry) {
                    return Promise
                        .delay(this._retryAttempt(attempt))
                        .bind(this)
                        .call('_fetchPage', attempt);
                }

                throw err;
            });
    }

    return promise.bind(this).then(function processElasticResponse(response) {
        // set new scroll id
        var nextId = response && response._scroll_id;
        if (nextId) {
            this.scrollId = nextId;
        } else {
            throw new Error('scrollId was not returned from elasticsearch');
        }

        var hits = response.hits;
        var docs = hits.hits,
            total = hits.total;
        var length = docs.length;

        if (this._next !== false && length > 0) {
            // push into stream
            docs.forEach(function (doc) { this.push(doc); }, this);
        }

        this.streamed += length;
        if (this._next !== false && this.streamed >= total) {
            this._next = false;
            this.push(null);
        }
    })
    .catch(function scrollError(err) {
        this._next = false;
        this.emit('error', err);
    });
};

ESScrollStream.prototype.destroy = function () {
    if (this.destroyed) {
        return;
    }

    this.destroyed = true;
    this._next = false;
    this.unpipe();
};

module.exports = ESScrollStream;
