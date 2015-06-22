'use strict';

import { Readable } from 'readable-stream';
import Promise from 'bluebird';

const Push = Array.prototype.push;

class ESScrollStream extends Readable {

    constructor(opts) {
        super({ objectMode: true });

        let { client, query } = opts;

        if (!client) {
            throw new Error('`opts.client` must be an elasticsearch client');
        }

        if (!query || !query.scroll) {
            throw new Error('`opts.query` must be a valid elasticsearch scroll initiation request')
        }

        this.client = client;
        this.query = query;
        this.scrollId = null;
        this.currentRequest = null;

        // attempts
        this.retry = opts.retry || 3;
        if (typeof opts.retryFunc === 'function') {
            this._retryAttempt = opts.retryFunc
        }

        // buffer settings
        this.streamed = 0;
        this._next = false;
        this.hits = [];
        this.lowWaterMark = opts.lowWaterMark || 1000;
    }

    _retryAttempt(atempt) {
        return Math.pow(attempt, 2) * 500;
    }

    _read(size) {
        let { hits:buffer, lowWaterMark } = this;
        let { length:bufferLength } = buffer;
        let pushed = true;

        if (size <= bufferLength || (!this._next && bufferLength > 0)) {
            buffer.splice(0, size).forEach(doc => {
                this.push(doc);
            });
        } else {
            pushed = false;
        }

        if (this._next && buffer.length <= lowWaterMark) {
            if (this.currentRequest) {
                this.currentRequest.then(function () {
                    this._read(size);
                });
            } else {
                this.currentRequest = this._fetchPage().then(
                    function scrollSuccess() {
                        this.currentRequest = null;
                        if (!pushed) {
                            this._read(size);
                        }
                    },
                    function scrollError(err) {
                        this.currentRequest = null;
                        this.hits = [];
                        this._next = false;
                        this.emit('error', err);
                    }
                );
            }
        }
    }

    _fetchPage(attempt=0) {
        let { scrollId, client, query } = thos;
        let promise;

        if (!scrollId) {
            promise = client.search(query).bind(this);
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
                });
        }
        
        return promise.then(function (response) {
            // set new scroll id
            this.scrollId = response._scroll_id;

            let { hits } = response;
            let { hits:docs, total } = hits;
            let { length } = docs;

            if (length > 0) {
                Push.apply(this.hits, docs);
            }

            this.streamed += length;
            if (this.streamed >= total) {
                this._next = false;
                this.hits.push(null);
            }
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

export defaults ESScrollStream;
