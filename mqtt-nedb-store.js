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
  this.db = new Datastore({ filename: this._opts.filename, autoload: true })
  if (this._opts.autocompactionInterval) {
    this.db.persistence.setAutocompactionInterval(Number(this._opts.autocompactionInterval))
  }
}

/**
 * Adds a packet to the store, a packet is
 * anything that has a messageId property.
 *
 */
Store.prototype.put = function (packet, cb) {
  cb = cb || noop
  this.db.insert({_id: packet.messageId, packet: packet}, function (err, doc) {
    if (err) {
      return this.db.update({_id: packet.messageId}, {_id: packet.messageId, packet: packet}, {}, function (err) {
        cb(err, packet)
      }.bind(this))
    }
    cb(null, doc)
  }.bind(this))
  return this
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
      cb(null, packet.packet)
    } else {
      cb(err || new Error('missing packet'))
    }
  });
  return this
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
  var incomingOptions = (options && options.incoming) || {}
  incomingOptions.filename = require('path').join(path, 'incoming')
  this.incoming = new Store(incomingOptions)

  var outgoingOptions = (options && options.outgoing) || {}
  outgoingOptions.filename = require('path').join(path, 'outgoing')
  this.outgoing = new Store(outgoingOptions)
}

Manager.single = Store;

Manager.prototype.close = function (done) {
  this.incoming.close();
  this.outgoing.close();

  done && done() // jshint ignore:line
};

module.exports = Manager;
