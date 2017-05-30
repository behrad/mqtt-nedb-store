'use strict';

var mqtt = require('mqtt'),
  nedbStore = require('./'),
  manager = nedbStore('db'),
  client;

client = mqtt.connect({
  port: 1883,
  incomingStore: manager.incoming,
  outgoingStore: manager.outgoing
});

client.on('connect', function () {
  console.log('connected');
  client.publish('hello', 'world', {qos: 1}, function () {
    console.log('published');
    client.end();
  });
});
