var MongoClient = require('mongodb').MongoClient;
var Mongopenter = require('../lib/mongopenter');
var async = require('async');
var should = require('should');
var testDb = 'mongodb://localhost/admin';
var testDbNames = [ 'databaseName', 'anotherDatabaseName' ];
var conn;

function getCollectionNames(db, cb) {
  db.collectionNames(function (err, collectionNames) {
    if (err) return cb(err);
    cb(null, collectionNames.map(function (collectionName) {
      return collectionName.name ? collectionName.name : collectionName;
    }));
  });
}

function collectionShouldContainKeys(db, collection, expectedKeys, cb) {
  db.collection(collection, function (err, collection) {
    collection.find({}, function (err, docs) {
      docs.toArray(function (err, docs) {
        var keys = [];
        docs.forEach(function (doc) {
          keys = keys.concat(Object.keys(doc));
        });

        expectedKeys.forEach(function (expectedKey) {
          keys.should.include(expectedKey);
        })
        cb();
      });
    });
  });
}

describe('Mongpenter', function () {
  before(function (done) {
    MongoClient.connect(testDb, function (err, db) {
      if (err) done(err);
      conn = db;
      done();
    });
  });

  it('should create the example structure correctly', function (done) {
    new Mongopenter(testDb, { setupFile: 'examples/mongopenter.json' }).setup(function (err, results) {
      function checkDatabaseName(cb) {
        var db = conn.db('databaseName');
        getCollectionNames(db, function (err, collectionNames) {
          collectionNames.should.include('databaseName.aCollection');
          collectionNames.should.include('databaseName.anotherCollection');
          collectionShouldContainKeys(db, 'aCollection', ['aKey1', 'aKey2', 'aKey3'], cb);
        });
      }

      function checkAnotherDatabaseName(cb) {
        var db = conn.db('anotherDatabaseName');
        getCollectionNames(db, function (err, collectionNames) {
          collectionNames.should.include('anotherDatabaseName.yetAnotherCollection');
          collectionShouldContainKeys(db, 'yetAnotherCollection', ['oh'], cb);
        });
      }

      async.series([ checkDatabaseName, checkAnotherDatabaseName ], done);
    });
  });

  after(function (done) {
    async.eachSeries(testDbNames, function (dbName, cb) {
      var newDb = conn.db(dbName);
      newDb.dropDatabase(cb);
    }, function (err) {
      conn.close();
      done(err);
    });
  });
});
