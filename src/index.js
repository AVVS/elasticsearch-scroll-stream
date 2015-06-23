'use strict';

import { Readable } from 'readable-stream';
import Promise from 'bluebird';

class ESScrollStream extends Readable {

    constructor(opts) {
        super({
            objectMode: true,
            highWaterMark: opts.highWaterMark || 1000
        });

        let { client, query } = opts;

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
        this.lowWaterMark = this.lowWaterMark || 1000;
    }

    _retryAttempt(attempt) {
        return Math.pow(attempt, 2) * 500;
    }

    _read() {
        if (this.currentRequest) {
            this.pushRequested = true;
            return;
        }

        this.currentRequest = this._fetchPage().finally(function () {
            this.currentRequest = null;

            if (this._next !== false && (this.pushRequested || this._readableState.buffer.length < this.lowWaterMark)) {
                this.pushRequested = false;
                return this._read();
            }
        });
    }

    _fetchPage(attempt=0) {
        let { scrollId, client, query } = this;
        let promise;

        if (!scrollId) {
            promise = client.search(query).bind(this).tap(function (resp) {
                this.emit('log', 'total elements ' + resp.hits.total);
            });
        } else {
            promise = client
                .scroll({ body: scrollId, scroll: query.scroll })
                .bind(this)
                .catch(function (err) {
                    if (++attempt < this.retry) {
                        return Promise.delay(this._retryAttempt(attempt))
                            .bind(this)
                            .then(function nextRetryAttempt() {
                                return this._fetchPage(attempt);
                            });
                    }

                    throw err;
                });
        }

        return promise.then(function (response) {
            // set new scroll id
            this.scrollId = response._scroll_id;

            let { hits } = response;
            let { hits: docs, total } = hits;
            let { length } = docs;

            if (this._next !== false && length > 0) {
                docs.forEach(function (doc) {
                    this.push(doc);
                }, this);
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
    }

    destroy() {
        if (this.destroyed) {
            return;
        }

        this.destroyed = true;
        this._next = false;
        this.hits = [];
        this.unpipe();
    }
}

export default ESScrollStream;
