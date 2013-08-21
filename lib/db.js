var mongodb = require('mongodb');
var async = require('async');
var fs = require('fs');

exports = module.exports = Db;

function Db(mongopenter, opts) {
  this.mongopenter = mongopenter;
  this.urls = opts.urls;
  this.setup = opts.setup;
  this.shards = opts.shards;
  this.databases = this._getDatabases();

  var collectionsAndDocuments = this._getCollectionsAndDocuments();
  this.collections = collectionsAndDocuments.collections;
  this.documents = collectionsAndDocuments.documents;
}

Db.prototype._getDatabases = function () {
  var result = [];
  var databases = this.setup.databases;
  var databaseNames;
  if (databases && (databaseNames = Object.keys(databases)).length > 0) {
    databaseNames.forEach(function (databaseName) {
      result.push({ name: databaseName, options: databases[databaseName] });
    });
  }
  return result && result.length > 0 ? result : undefined;
};

Db.prototype._getCollectionsAndDocuments = function () {
  var databases = this.databases;
  var collections = [];
  var documents = [];
  if (databases) {
    var self = this;
    databases.forEach(function (database) {
      var options = database.options;
      if (options.collections) {
        options.collections.forEach(function (collection) {
          var collectionName = (typeof(collection) === 'object' ? collection.name : collection);
          collections.push({ db: database.name, name: collectionName });
          if (typeof(collection) === 'object' && collection.docs) {
            collection.docs.forEach(function (doc) {
              var unrolledDoc = self._unrollDocument(doc);
              documents.push({ db: database.name, collection: collectionName, query: unrolledDoc.query, doc: unrolledDoc.doc });
            });
          }
        });
      }
    });
  }
  return {
    collections: (collections.length > 0 ? collections : undefined),
    documents: (documents.length > 0 ? documents : undefined)
  };
};

Db.prototype._unrollDocument = function (doc) {
  if (typeof(doc) === 'string') {
    var loadedDoc = require(this.mongopenter.resolveFile(doc));
    var queryBody = typeof(loadedDoc.query) === 'function' ? loadedDoc.query() : loadedDoc.query;
    var docBody = typeof(loadedDoc.doc) === 'function' ? loadedDoc.doc() : loadedDoc.doc;
    return { query: queryBody, doc: docBody };
  }

  var unrolledDoc = { query: doc.query };
  if (typeof(doc.doc) === 'string') {
    unrolledDoc.doc = JSON.parse(fs.readFileSync(this.mongopenter.resolveFile(doc.doc)));
  } else {
    unrolledDoc.doc = doc.doc;
  }
  return unrolledDoc;
};

Db.prototype.createDatabases = function (cb) {
  var databases = this.databases;
  if (databases) {
    console.log('Mongopenter: Creating databases...');
    var self = this;
    self.getConnection(function (err, db) {
      if (err) return cb(err);
      var jobs = databases.map(function (database) {
        return self._createDatabase.bind(self, db, database);
      });
      async.series(jobs, function (err, results) {
        db.close();
        cb(err, results);
      });
    });
  } else {
    process.nextTick(cb);
  }
};

Db.prototype._createDatabase = function (db, database, cb) {
  var newDb = db.db(database.name);
  var users = [ this.setup.auth, database.options.auth ];
  var jobs = [];
  users.forEach(function (auth) {
    if (!auth) return;
    jobs.push(function (cb) {
      newDb.addUser(auth.user, auth.password, function (err, user) {
        if (err) console.error('Mongopenter:  - Failed to add user to database', auth.user, err);
        cb(err, user);
      });
    });
  });
  async.series(jobs, cb);
};

Db.prototype.createCollections = function (cb) {
  var collections = this.collections;
  if (collections) {
    console.log('Mongopenter: Creating collections...');
    var self = this;
    this.getConnection(function (err, db) {
      if (err) return cb(err);
      var jobs = [];
      collections.forEach(function (collection) {
        jobs.push(self._createCollection.bind(self, db, collection));
      });

      async.series(jobs, function (err, results) {
        db.close();
        cb(err, results);
      });
    });
  } else {
    process.nextTick(cb);
  }
};

Db.prototype._createCollection = function (db, collection, cb) {
  var newDb = db.db(collection.db);
  newDb.collectionNames(function (err, collectionNames) {
    if (err) return cb(err);
    collectionNames = collectionNames.map(function (collectionName) {
      // The documentation for node-mongodb-driver defines this as returning [ 'name', 'name' ]
      // but the current version returns [ { name: 'name' }, { name: 'name' } ]
      return collectionName.name ? collectionName.name : collectionName;
    });
    if (collectionNames.indexOf(collection.db + '.' + collection.name) === -1) {
      newDb.createCollection(collection.name, function (err, collection) {
        if (err) console.error('Mongopenter:  - Failed to create collection', err);
        cb(err, collection);
      });
    } else {
      cb();
    }
  });
};

Db.prototype.createDocuments = function (cb) {
  var documents = this.documents;
  if (documents) {
    console.log('Mongopenter: Creating documents...');
    var jobs = [];
    var self = this;
    this.getConnection(function (err, db) {
      if (err) return cb(err);
      documents.forEach(function (document) {
        jobs.push(self._createDocument.bind(self, db, document));
      });

      async.series(jobs, function (err, results) {
        db.close();
        cb(err, results);
      });
    });
  } else {
    process.nextTick(cb);
  }
};

Db.prototype._createDocument = function (db, doc, cb) {
  var newDb = db.db(doc.db);
  newDb.collection(doc.collection, function (err, collection) {
    if (err) return cb(err);
    collection.findOne(doc.query, function (err, item) {
      if (err) {
        console.error('Mongopenter:  - Failed to create document', doc.query, ':', err);
        return cb(err);
      }
      if (item) return cb(null, item);
      collection.insert(doc.doc, cb);
    });
  });
};


Db.prototype.createShards = function (cb) {
  var shardAddresses = this.shards ? this.shards.hosts : undefined;
  if (shardAddresses) {
    console.log('Mongopenter: Creating shards...');
    var jobs = [];
    var self = this;
    this.getConnection(function (err, db) {
      shardAddresses.forEach(function (shardAddress) {
        jobs.push(self._createShard.bind(self, db, shardAddress));
      });
      async.series(jobs, function (err, results) {
        db.close();
        cb(err, results);
      });
    });
  } else {
      process.nextTick(cb);
  }
};

Db.prototype._createShard = function(db, shardAddress, cb) {
  console.log(" - Creating shard: " + shardAddress);
  db.command({ addshard:shardAddress }, function(err, result) {
    if (result.errmsg) return cb(err);
    cb(null, result);
  });
};

Db.prototype.addShards = function (cb) {
  if (this.setup.shards && this.setup.shards.shardTags) {
    console.log('Mongopenter: Adding shards tags...');
    var self = this;
    var shardTags = self.setup.shards.shardTags;
    var shardDb = self.setup.shards.shardDb;
    var shardCollection = self.setup.shards.shardCollection;
    var shardKey = self.setup.shards.shardKey;
    var keyMap = self.shards.keyMap;

    var jobs = [];
    this.getConnection(function (err, db) {
      shardTags.forEach(function (shardTag) {
        jobs.push(self._addShardTag.bind(self, db, shardTag));
      });
      jobs.push(self._enableSharding.bind(self, db, shardDb));
      jobs.push(self._shardCollection.bind(self, db, shardCollection, shardKey));

      Object.keys(self.shards.keyMap).forEach(function(host) {
        var tag = self.shards.keyMap[host];
        jobs.push(self._addTagRanges.bind(self, db, shardCollection, tag));
      });

      async.series(jobs, function (err, results) {
        db.close();
        cb(err, results);
      });
    });
  } else {
      process.nextTick(cb);
  }
};

Db.prototype._addShardTag = function(db, shardTag, cb) {
  console.log(" - Adding tag: " + shardTag.tag + " to shard: " + shardTag.shard);
  var config = db.db("config");
  config.collection("shards", function (err, configCollection) {
    if (err) return cb(err);
    configCollection.findOne( { _id : shardTag.shard }, function (err, item) {
      if (err || !item) return cb(err);
      configCollection.update( { _id: shardTag.shard }, { $addToSet: { tags: shardTag.tag } }, function (err, item) {
        if (err) return cb(err);
        cb(undefined, item);
      });
    });
  });
};

Db.prototype._enableSharding = function(db, shardDb, cb) {
  console.log(" - Enable sharding on: " + shardDb);
  db.command({ enableSharding : shardDb}, function(err, result) {
    if (result.errmsg) return cb(err);
    cb(null, result);
  });
};


Db.prototype._shardCollection = function(db, shardCollection, shardKey, cb) {
  console.log(" - Sharding collection: " + shardCollection + " on field: " + shardKey);
  db.command({shardCollection : shardCollection, key: {shardKey: 1}}, function(err, result) {
    if (result.errmsg) return cb(err);
    cb(null, result);
  });
};


Db.prototype._addTagRanges = function(db, ns, tag, cb) {
  var min = { "shardKey": tag };
  var max = { "shardKey": tag + 'z' };
  console.log(" - Adding ranges: " + JSON.stringify(min) + " - " + JSON.stringify(max) + " to tag: " + tag);
  var config = db.db("config");
  config.collection("tags", function (err, tagsCollection) {
    if (err) return cb(err);
    tagsCollection.update( {_id: { ns : ns , min : min } }, { $set: { ns : ns , min : min , max : max , tag : tag }},  { upsert: true },
      function (err, item) {
        if (err) {
          console.error(' - Failed to add tag', err);
          cb(err);
        } else {
          cb(undefined, item);
        }
      });
  });
};

Db.prototype.getConnection = function (cb) {
  mongodb.connect(this.urls, function (err, db) {
    if (err) console.error(' - Failed to get connection', err);
    cb(err, db);
  });
};
