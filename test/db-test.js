var Db = require('../lib/db');
var should = require('should');
var sinon = require('sinon');
var path = require('path');
var mongodb = require('mongodb');

describe('Db', function () {

  beforeEach(function() {
    this.sinon = sinon.sandbox.create();
    this.consoleInfoStub = this.sinon.stub(console, 'log');
    this.consoleErrorStub = this.sinon.stub(console, 'error');
    this.collectionStub = { findOne: this.sinon.stub(),
                            insert: this.sinon.stub(),
                            update: this.sinon.stub() };
    this.dbStub = { addUser: this.sinon.stub(),
                    collectionNames: this.sinon.stub(),
                    createCollection: this.sinon.stub(),
                    collection: this.sinon.stub()
    };
    this.connectionStub = { db: this.sinon.stub().returns(this.dbStub),
      collection: this.sinon.stub(),
      close: this.sinon.stub(),
      command: this.sinon.stub()
    };
    this.mongopenter = {
      resolveFile: function (aPath) {
        return path.resolve(__dirname, '../', aPath);
      }
    };
    this.cbStub = this.sinon.stub();
  });

  afterEach(function() {
    this.sinon.restore();
  });

  describe('#Db', function () {
    it('should load database configuration properly', function () {
      var expectedDatabases = [
        { name: 'aDatabase', options: { empty: true } },
        { name: 'anotherDatabase', options: { empty: true } }
      ];
      var setupDatabases = {};
      setupDatabases[expectedDatabases[0].name] = expectedDatabases[0].options;
      setupDatabases[expectedDatabases[1].name] = expectedDatabases[1].options;

      var opts = { urls: 'aUrl', setup: { databases: setupDatabases }};

      var db = new Db(this.mongopenter, opts);
      db.databases.should.eql(expectedDatabases);
    });

    it('should not explode if configuration is empty', function () {
      var db = new Db(this.mongopenter, { setup: {}});
      should.not.exist(db.databases);
    });

    it('should load collection and document configuration properly', function () {
      var anotherCollectionDocs = [
        {
          query: {
            aKey2: 'aValue2'
          },
          doc: {
            aKey2: 'aValue2'
          }
        },
        './test/fixtures/aDocWithExports',
        './test/fixtures/aDocWithStringExports',
        {
          query: {
            aKey1: 'aValue1'
          },
          doc: './test/fixtures/aDoc.json'
        }
      ];

      var aDatabaseCollections = [
        { name: 'aCollection' },
        { name: 'anotherCollection', docs: anotherCollectionDocs }
      ];

      var anotherDatabaseCollections = [
        'emptyCollection'
      ];

      var expectedDatabases = [
        { name: 'aDatabase', options: { collections: aDatabaseCollections } },
        { name: 'anotherDatabase', options: { collections: anotherDatabaseCollections } }
      ];
      var setupDatabases = {};
      setupDatabases[expectedDatabases[0].name] = expectedDatabases[0].options;
      setupDatabases[expectedDatabases[1].name] = expectedDatabases[1].options;

      var opts = { urls: 'aUrl', setup: { databases: setupDatabases }};

      var db = new Db(this.mongopenter, opts);
      db.databases.should.eql(expectedDatabases);
      db.collections.should.eql([
        { db: 'aDatabase', name: 'aCollection' },
        { db: 'aDatabase', name: 'anotherCollection' },
        { db: 'anotherDatabase', name: 'emptyCollection' }
      ]);
      db.documents.should.eql([
        {
          db: 'aDatabase',
          collection: 'anotherCollection',
          query: {
            aKey2: 'aValue2'
          },
          doc: {
            aKey2: 'aValue2'
          }
        },
        {
          db: 'aDatabase',
          collection: 'anotherCollection',
          query: {
            aKey3: 'aValue3'
          },
          doc: {
            aKey3: 'aValue3'
          }
        },
        {
          db: 'aDatabase',
          collection: 'anotherCollection',
          query: {
            aKey3: 'aValue3'
          },
          doc: {
            aKey3: 'aValue3'
          }
        },
        {
          db: 'aDatabase',
          collection: 'anotherCollection',
          query: {
            aKey1: 'aValue1'
          },
          doc: {
            aKey1: 'aValue1'
          }
        }
      ]);
    });
  });

  describe('#createDatabases', function () {
    it('should add users to the new database', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: { auth: { user: 'abba', password: 'babba' } } });
      db.databases = [
        { name: 'adatabase', options: { auth: { user: 'cheese', password: 'bacon' }} },
        { name: 'anotherdatabase', options: {} }
      ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);
      this.dbStub.addUser.callsArgWith(2, null, 'oh i lied');

      db.createDatabases(this.cbStub);
      connectStub.calledWith('aUrl').should.eql(true);
      this.connectionStub.db.calledWith('adatabase').should.eql(true);
      this.connectionStub.db.calledWith('anotherdatabase').should.eql(true);
      this.dbStub.addUser.calledWith('abba', 'babba').should.eql(true);
      this.dbStub.addUser.calledWith('cheese', 'bacon').should.eql(true);
      this.cbStub.called.should.eql(true);
    });

    it('should log if addUser fails', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: { } });
      db.databases = [
        { name: 'adatabase', options: { auth: { user: 'cheese', password: 'bacon' }} }
      ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);
      this.dbStub.addUser.callsArgWith(2, 'done borked', 'oh i lied');

      db.createDatabases(this.cbStub);
      connectStub.calledWith('aUrl').should.eql(true);
      this.consoleErrorStub.calledWith('Mongopenter:  - Failed to add user to database', 'cheese', 'done borked').should.eql(true);
      this.cbStub.called.should.eql(true);
    });

    it('should not die if no auth', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.databases = [ { name: 'adatabase', options: {} } ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);

      db.createDatabases(this.cbStub);
      connectStub.calledWith('aUrl').should.eql(true);
      this.cbStub.called.should.eql(true);
    });

    it('should not die if no databases specified', function (done) {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.createDatabases(function () {
        done();
      });
    });

    it('should callback with errors getting connection', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.databases = [];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, 'i be broken');

      db.createDatabases(this.cbStub);
      this.cbStub.calledWith('i be broken').should.eql(true);
    });
  });

  describe('#createCollections', function () {
    it('should not create collections that already exist', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.collections = [
        { db: 'adb', name: 'acollection' },
        { db: 'anotherDb', name: 'anotherCollection' }
      ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);
      this.dbStub.collectionNames.callsArgWith(0, null, [ 'anotherDb.anotherCollection', { name: 'dodgyMongoDbDriverDoc' } ]);
      this.dbStub.createCollection.callsArgWith(1, null, 'aCollection');

      db.createCollections(this.cbStub);
      this.dbStub.createCollection.calledWith('acollection').should.eql(true);
      this.dbStub.createCollection.calledWith('anotherCollection').should.eql(false);
      this.cbStub.called.should.eql(true);
    });

    it('should not explode if no collections specified', function (done) {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.collections = undefined;
      db.createCollections(function () {
        done();
      });
    });

    it('should callback with errors getting connection', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.collections = [];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, 'i be broken');

      db.createCollections(this.cbStub);
      this.cbStub.calledWith('i be broken').should.eql(true);
    });

    it('should callback with errors getting current collection names', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.collections = [ { db: 'adb', name: 'acollection' } ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);
      this.dbStub.collectionNames.callsArgWith(0, 'darn broked it');

      db.createCollections(this.cbStub);
      this.cbStub.calledWith('darn broked it').should.eql(true);
    });

    it('should log errors creating collections', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.collections = [ { db: 'adb', name: 'acollection' } ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);
      this.dbStub.collectionNames.callsArgWith(0, null, []);
      this.dbStub.createCollection.callsArgWith(1, 'bugger');

      db.createCollections(this.cbStub);
      this.consoleErrorStub.calledWith('Mongopenter:  - Failed to create collection', 'bugger').should.eql(true);
      this.cbStub.calledWith('bugger').should.eql(true);
    });
  });

  describe('#createDocuments', function () {
    it('should not create documents that already exist', function () {
      var db = new Db(this.mongopenter, { setup: {} });
      db.documents = [
        { db: 'adb', collection: 'acollection', query: 'aquery', doc: 'adoc' },
        { db: 'anotherdb', collection: 'anothercollection', query: 'anotherquery', doc: 'anotherdoc' }
      ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);
      this.dbStub.collection.callsArgWith(1, null, this.collectionStub);
      this.collectionStub.findOne.withArgs('aquery').callsArgWith(1);
      this.collectionStub.findOne.withArgs('anotherquery').callsArgWith(1, null, 'anotherdoc');
      this.collectionStub.insert.callsArgWith(1);

      db.createDocuments(this.cbStub);
      this.collectionStub.insert.calledWith('adoc').should.eql(true);
      this.collectionStub.insert.calledWith('anotherdoc').should.eql(false);
    });

    it('should not explode if no documents specified', function (done) {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.documents = undefined;
      db.createDocuments(function () {
        done();
      });
    });

    it('should callback with errors getting connection', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.documents = [];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, 'i be broken');

      db.createDocuments(this.cbStub);
      this.cbStub.calledWith('i be broken').should.eql(true);
    });

    it('should callback with errors getting collection', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.documents = [ { query: 'a', doc: 'b' }];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);
      this.dbStub.collection.callsArgWith(1, 'doh');

      db.createDocuments(this.cbStub);
      this.cbStub.calledWith('doh').should.eql(true);
    });

    it('should callback with errors finding existing doc', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.documents = [ { query: 'a', doc: 'b' }];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);
      this.dbStub.collection.callsArgWith(1, null, this.collectionStub);
      this.collectionStub.findOne.callsArgWith(1, 'BAD');

      db.createDocuments(this.cbStub);
      this.consoleErrorStub.calledWith('Mongopenter:  - Failed to create document', 'a', ':', 'BAD').should.eql(true);
      this.cbStub.calledWith('BAD').should.eql(true);
    });
  });

  describe('#createShards', function () {
    it('should create shards to the new database', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl',
        shards: {hosts: ['1.1.1.1', '2.2.2.2']},
        setup: { } });

      db.databases = [
        { name: 'adatabase', options: { } }
      ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);

      this.connectionStub.command.callsArgWith(1, null, "res");
      db.createShards(this.cbStub);

      connectStub.calledWith('aUrl').should.eql(true);
      this.connectionStub.command.calledWith({addshard: '1.1.1.1'}).should.eql(true);
      this.connectionStub.command.calledWith({addshard: '2.2.2.2'}).should.eql(true);
    });

    it('should just callback if no shards specified', function (done) {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.createShards(function () {
        done();
      });
    });

    it('should call back with errors if thrown', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl',
        shards: {hosts: ['1.1.1.1', '2.2.2.2']},
        setup: { } });

      db.databases = [
        { name: 'adatabase', options: { } }
      ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);

      this.connectionStub.command.callsArgWith(1, null, { errmsg: "ohgodno" });
      db.createShards(this.cbStub);

      connectStub.calledWith('aUrl').should.eql(true);
      this.connectionStub.command.calledWith({addshard: '1.1.1.1'}).should.eql(true);
      this.cbStub.calledWith('ohgodno').should.eql(true);
    });
  });

  describe('#addShards', function () {
    it('should add shards to the new database', function () {
      var db = new Db(this.mongopenter, { urls: 'aUrl',
        shards: { keyMap: {'mac': 'apple', 'ubuntu': 'banana'}},
        setup: { shards : { shardDb: 'adatabase',
                            shardCollection: 'acollection',
                            shardKey: 'akey',
                            shardTags: [{'shard': 'shard0000', 'tag': 'apple'}]}} });
      db.databases = [
        { name: 'adatabase', options: { } }
      ];

      var connectStub = this.sinon.stub(mongodb, 'connect');
      connectStub.callsArgWith(1, null, this.connectionStub);

      this.dbStub.collection.callsArgWith(1, null, this.collectionStub);

      this.collectionStub.findOne.withArgs({_id: 'shard0000'}).callsArgWith(1, null, {a:1});
      this.collectionStub.update.callsArgWith(2, null, {});
      this.connectionStub.command.callsArgWith(1, null, "res");

      var updateStub = {update: this.sinon.stub()};
      this.dbStub.collection.callsArgWith(1, null, updateStub);

      db.addShards(this.cbStub);

      this.collectionStub.findOne.called.should.eql(true);
      this.collectionStub.update.calledWith({_id: 'shard0000'}).should.eql(true);
      this.connectionStub.command.calledWith({enableSharding: 'adatabase'}).should.eql(true);
      this.connectionStub.command.calledWith({shardCollection: 'acollection', key: {shardKey: 1}}).should.eql(true);
      updateStub.update.called.should.eql(true);
    });

    it('should just callback if no shards to be added', function (done) {
      var db = new Db(this.mongopenter, { urls: 'aUrl', setup: {} });
      db.addShards(function () {
        done();
      });
    });
  });

});
