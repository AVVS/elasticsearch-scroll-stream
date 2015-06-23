'use strict';

require('babel/register')();

var chai = require('chai');
var expect = chai.expect;

describe('es-scroll-stream', function () {

    var Promise = require('bluebird');
    var elasticsearch = require('elasticsearch');
    var faker = require('faker');
    var _ = require('lodash');
    var ESStream = require('../src/index.js');

    before('init es client', function () {
        this.elasticsearch = new elasticsearch.Client({
            host: 'localhost:9200'
        });

        return this.elasticsearch.ping();
    });

    before('create es index', function () {
        return this.elasticsearch.indices.create({ index: 'test-index', body: {
            settings: {
                index: {
                   'number_of_shards': 1,
                   'number_of_replicas': 0,
                   'auto_expand_replicas': '0'
                }
            }
        }});
    });

    before('populate es index', function () {
        var commands = [];
        _.times(5000, function () {
            commands.push(
                { index: { _index: 'test-index', _type: 'test-type' } },
                faker.helpers.createCard()
            );
        });

        return this.elasticsearch.bulk({ body: commands, refresh: true });
    });

    it('able to iterate through created data set', function () {
        return new Promise(function (resolve, reject) {
            var stream = new ESStream({
                client: this.elasticsearch,
                query: {
                    index: 'test-index',
                    type: 'test-type',
                    scroll: '30s',
                    body: {
                        query: {
                            'match_all': {}
                        },
                        size: 750
                    }
                },
                lowWaterMark: 1000,
                highWaterMark: 2500
            });

            var consumed = false;
            var requestsInProgress = 0;
            var objects = [];

            function onEnd() {
                consumed = true;
                if (requestsInProgress === 0) {
                    expect(objects.length).to.be.eq(5000);
                    resolve();
                }
            }

            function readable() {
                return Promise.resolve(stream.read()).then(function (object) {
                    if (!object) {
                        return null;
                    }

                    requestsInProgress++;

                    return Promise.delay(300).then(function () {
                        objects.push(object);

                        if (--requestsInProgress === 0 && consumed) {
                            return resolve();
                        }

                        return readable();
                    });
                });
            }

            function onError(err) {
                stream.removeListener('end', onEnd);
                stream.removeListener('readable', readable);
                stream.destroy();
                reject(err);
            }

            stream.on('error', onError);
            stream.on('end', onEnd);
            stream.on('readable', readable);

            // init read
            readable();
        }.bind(this));
    });

    after('destroy es index', function () {
        return this.elasticsearch.indices.delete({ index: 'test-index' });
    });

});
