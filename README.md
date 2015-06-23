# Elasticsearch scroll stream

Node.js stream adapter for elasticsearch scrolling. Often scrolling is heavier
than the underlaying operations, so what we do here is make sure that there is always
a pool of documents to read from, and when low watermark is hit we do another es
scroll request, that way there are always docs to process and we make sure that we
are cpu bound and not IO bound

## Install

`npm install -S elasticsearch-scroll-stream`

## Usage

```js
'use strict';

import Promise from 'bluebird';
import ESStream from 'elasticsearch-scroll-stream';
import elasticsearch from 'elasticsearch';

const ES_SHARDS = 144;
const ES_EXHAUST_RATIO = 3;
const esClient = new elasticsearch.Client(...);
const query = {
    index: 'name_of_index',
    type: 'type',
    scroll: '60s',
    body: {
        query: {
            match_all: {}
        },
        size: 50
    }
};
const opts = {
    client: esClient,
    query: query,
    // we need to make sure that ES scroll request is completed before we exhaust buffers
    highWaterMark: query.body.size * ES_SHARDS * ES_EXHAUST_RATIO
};

const readableStream = new ESStream(opts);
let dataReady, streamConsumed, streamError;

streamConsumed = function () {
    callback();
}

streamError = function (err) {
    readableStream.destroy();
    callback(err);
}

readableStream.on('end', streamConsumed);
readableStream.on('error', streamError);
readable.on('readable', function processData() {
    let chunk = readable.read(100);
    // do smth
});

```