'use strict';

var abstractTest = require('mqtt/test/abstract_store'),
  mqttNedbStore = require('./'),
  mqtt = require('mqtt'),
  concat = require('concat-stream');

describe('mqtt nedb store', function () {
  abstractTest(function (done) {
    done(null, mqttNedbStore.single('/tmp/.mqtt-nedb-store', {}));
  });
});

describe('mqtt nedb store manager', function () {
  var manager;

  beforeEach(function () {
    manager = mqttNedbStore('/tmp/.mqtt-nedb-store', {});
  });

  afterEach(function (done) {
    manager.close(done);
  });

  describe('incoming', function () {
    abstractTest(function (done) {
      done(null, manager.incoming);
    });
  });

  describe('outgoing', function () {
    abstractTest(function (done) {
      done(null, manager.outgoing);
    });
  });
});

describe('mqtt.connect flow', function () {
  var server,
    manager;

  beforeEach(function (done) {
    server = new mqtt.Server();
    server.listen(8883, done);

    server.on('client', function (client) {
      client.on('connect', function () {
        client.connack({returnCode: 0});
      });
    });

    manager = mqttNedbStore('/tmp/.mqtt-nedb-store', {});
  });

  it('should resend messages', function (done) {
    var client = mqtt.connect({
      port: 8883,
      incomingStore: manager.incoming,
      outgoingStore: manager.outgoing
    });

    client.publish('hello', 'world', {qos: 1});

    server.once('client', function (serverClient) {
      serverClient.once('publish', function () {
        serverClient.stream.destroy();

        manager.outgoing.createStream().pipe(concat(function (list) {
          list.length.should.equal(1);
        }));
      });

      server.once('client', function (serverClient2) {
        serverClient2.once('publish', function (packet) {
          serverClient2.puback(packet);
          client.end();
          done();
        });
      });
    });
  });
});
