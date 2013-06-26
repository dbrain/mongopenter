var MongoClient = require('mongodb').MongoClient;
var async = require('async');
var fs = require('fs');

exports = module.exports = Db;

function Db(mongopenter, opts) {
  this.mongopenter = mongopenter;
  this.urls = opts.urls;
  this.setup = opts.setup;
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
              if (unrolledDoc) {
                documents.push({ db: database.name, collection: collectionName, query: unrolledDoc.query, doc: unrolledDoc.doc });
              }
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
  console.log('Mongopenter: Creating databases...');
  var databases = this.databases;
  if (databases) {
    var self = this;
    self.getConnection(function (err, db) {
      var jobs = databases.map(function (database) {
        return self._createDatabase.bind(self, db, database);
      });
      async.series(jobs, function (err, results) {
        db.close();
        cb(err, results);
      });
    });
  } else {
    process.nextTick(cb.bind(this));
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
  console.log('Mongopenter: Creating collections...');
  var collections = this.collections;
  if (collections) {
    var self = this;
    this.getConnection(function (err, db) {
      if (err) return cb(err);
      var jobs = self._getCreateCollectionJobs(db);

      if (jobs.length > 0) {
        async.series(jobs, function (err, results) {
          db.close();
          cb(err, results);
        });
      } else {
        db.close();
        cb();
      }
    });
  } else {
    process.nextTick(cb.bind(this));
  }
};

Db.prototype._getCreateCollectionJobs = function (db) {
  var self = this;
  var collections = this.collections;
  var jobs = [];
  if (collections) {
    collections.forEach(function (collection) {
      var collectionName = (typeof(collection) === 'object' ? collection.name : collection);
      jobs.push(self._createCollection.bind(self, db, collection));
    });
  }
  return jobs;
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
  console.log('Mongopenter: Creating documents...');
  var documents = this.documents;
  if (documents) {
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
    process.nextTick(cb.bind(this));
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

Db.prototype.getConnection = function (cb) {
  MongoClient.connect(this.urls, function (err, db) {
    if (err) console.error(' - Failed to get connection', err);
    cb(err, db);
  });
};
