# MQTT.js NeDB Store

NeDB Store for in-flight MQTT.js packets. Thanks to [nedb](https://github.com/louischatriot/nedb).

## Usage

```js
'use strict';

var mqtt = require('mqtt'),
  NeDBStore = require('mqtt-nedb-store'),
  manager = NeDBStore('path/to/db');

var client = mqtt.connect({
  port: 8883,
  incomingStore: manager.incoming,
  outgoingStore: manager.outgoing
});

//// or
// var client = mqtt.connect('mqtt://test.mosca.io', {
//  port: 8883,
//  incomingStore: manager.incoming,
//  outgoingStore: manager.outgoing
//});

client.on('connect', function() {
  console.log('connected');
  client.publish('hello', 'world', {qos: 1}, function() {
    console.log('published');
    client.end();
  });
});
```

## License

MIT
