'use strict';

var Datastore = require('nedb'),
  Readable = require('readable-stream').Readable,
  streamsOpts = { objectMode: true },
  // msgpack = require('msgpack5'),
  noop = function () {};


/**
 * NeDB implementation of the message store
 *
 */
var Store = function (options) {
  if (!(this instanceof Store)) {
    return new Store(options)
  }

  this._opts = options || {}
  if (!this._opts.bprefix) this._opts.bprefix = "b:base64:"
  this.db = new Datastore({ filename: this._opts.filename, autoload: true })
}

/**
 * Adds a packet to the store, a packet is
 * anything that has a messageId property.
 *
 */
Store.prototype.put = function (packet, cb) {
  cb = cb || noop
  this.db.insert({_id: packet.messageId, packet: preparePacket.bind(this)(packet)}, function (err, doc) {
    if (err) {
      return this.db.update({_id: packet.messageId}, {_id: packet.messageId, packet: preparePacket.bind(this)(packet)}, {}, function (err) {
        cb(err, packet)
      }.bind(this))
    }
    cb(null, doc)
  }.bind(this))
  return this

  function preparePacket (packet){
  	var res = {}
  	for (var k in packet){
  		res[k] = bufferReplacer.bind(this)(packet[k])
    }
  	return res
  }

  function bufferReplacer (val) {
    if (!isBuffer(val)) return val
    var res = val.length ? this._opts.bprefix + val.toString("base64") : ''
    return res
  }

  function isBuffer (obj) {
    return !!obj.constructor && typeof obj.constructor.isBuffer === 'function' && obj.constructor.isBuffer(obj)
  }

}

/**
 * Creates a stream with all the packets in the store
 *
 */
Store.prototype.createStream = function () {
  var stream = new Readable(streamsOpts),
    destroyed = false,
    skip = 0,
    limit = this._opts.limit || 500,
    db = this.db;

  stream._read = function () {
    var _skip = skip
    skip += limit

    db
      .find({})
      .skip(_skip)
      .limit(limit)
      .exec(function (err, packets) {
        if ( err || destroyed || !packets || 0 === packets.length) {
          return this.push(null)
        }

        packets.forEach(function (packet) {
          this.push(packet.packet)
        }.bind(this))

      }.bind(this))
  }

  stream.destroy = function () {
    if (destroyed) {
      return
    }
    var self = this
    destroyed = true
    process.nextTick(function () {
      self.emit('close')
    })
  }
  return stream
}

/**
 * deletes a packet from the store.
 */
Store.prototype.del = function (packet, cb) {
  cb = cb || noop
  this.get(packet, function (err, packetInDb) {
    if (err) {
      return cb(err)
    }
    this.db.remove({ _id: packet.messageId }, {}, function (err) {
      cb(err, packetInDb)
    }.bind(this));
  }.bind(this))
  return this
}

/**
 * get a packet from the store.
 */
Store.prototype.get = function (packet, cb) {

  cb = cb || noop
  this.db.findOne({ _id: packet.messageId }, function (err, packet) {
    if (packet) {
      cb(null, packetReviver.bind(this)(packet.packet))
    } else {
      cb(err || new Error('missing packet'))
    }
  }.bind(this));
  return this

  function packetReviver (packet){
  	for (var k in packet)  
      packet[k] = bufferReviver.bind(this)(packet[k])
    return packet
  }

  function bufferReviver (val) { 
  	if (typeof val === 'string' && val.search(this._opts.bprefix) === 0 ){
	 	 return new Buffer(val.slice(this._opts.bprefix.length), 'base64')
	 }
	 else return val
  }
}

/**
 * Close the store
 */
Store.prototype.close = function (cb) {
  cb = cb || noop
  cb()
  return this
}

var Manager = function (path, options) {
  if (!(this instanceof Manager)) {
    return new Manager(path, options);
  }

  if ('object' === typeof path) {
    options = path
    path = null
  }

  if (!path) {
    path = require('path').join(require('os').homedir(), '.mqtt-nedb-store')
    try {
      require('fs').mkdirSync(path)
    } catch (e) {

    }
  }

  this.incoming = new Store({filename: require('path').join(path, 'incoming')})
  this.outgoing = new Store({filename: require('path').join(path, 'outgoing')})
}

Manager.single = Store;

Manager.prototype.close = function (done) {
  this.incoming.close();
  this.outgoing.close();

  done && done() // jshint ignore:line
};

module.exports = Manager;
