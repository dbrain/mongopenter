var Mongopenter = require('../lib/mongopenter').Mongopenter;
var should = require('should');
var sinon = require('sinon');
var path = require('path');
var async = require('async');

describe('Mongpenter', function () {

  beforeEach(function() {
    this.sinon = sinon.sandbox.create();
    this.mongopenter = new Mongopenter(null, { setupFile: 'test/fixtures/dummySetupFile.json' });
    this.cbStub = this.sinon.stub();
  });

  afterEach(function() {
    this.sinon.restore();
  });

  describe('#Mongopenter', function () {
    it('should not explode if empty constructor', function () {
      var mongopenter = new Mongopenter();
      should.not.exist(mongopenter.eventHandlers);
    });
  });

  describe('#init', function () {
    it('should proritise urls by parameter, env then default', function () {
      var mongopenter = new Mongopenter('imgoingtowin', {});
      mongopenter.options.urls.should.eql('imgoingtowin');

      var envStub = this.sinon.stub(process, 'env', { 'MONGODB_URL': 'nowimgoingtowin' });
      mongopenter = new Mongopenter(null, {});
      mongopenter.options.urls.should.eql('nowimgoingtowin');
      envStub.restore();

      mongopenter = new Mongopenter(null, {});
      mongopenter.options.urls.should.eql('mongodb://localhost/admin');
    });

    it('should proritise setupFile by parameter then default', function () {
      this.mongopenter.options.setupFile.should.eql(path.join(process.cwd(), 'test/fixtures/dummySetupFile.json'));

      mongopenter = new Mongopenter(null, {});
      mongopenter.options.setupFile.should.eql(path.join(process.cwd(), 'mongopenter.json'));
    });

    it('should read and parse the setupFile json and load scripts', function () {
      this.mongopenter.options.setup.test.should.eql('I came here for the cheese');
      this.mongopenter.eventHandlers.dummySetup.should.include('use me for tests');
    });

    it('should instantiate a db instance', function () {
      var mongopenter = new Mongopenter('blah', {});
      should.exist(mongopenter.db);
    });
  });

  describe('#_loadScripts', function () {
    it('should be cool with no scripts', function () {
      var mongopenter = new Mongopenter(null, { setupFile: 'test/fixtures/dummySetupFileNoScripts.json' });
      mongopenter.options.setup.test.should.eql('I came here for the bacon');
    });
  });

  describe('#on', function () {
    it('should add event handlers without overriding existing', function () {
      this.mongopenter.on('something', 'something');
      this.mongopenter.on('something', 'something1');
      this.mongopenter.eventHandlers.something.should.eql([ 'something', 'something1' ]);
    });
  });

  describe('#setup', function () {
    it('should call the expected tasks then notify', function () {
      var tasksStub = this.sinon.stub(this.mongopenter, '_tasks');
      var notifyStub = this.sinon.stub(this.mongopenter, '_notify');
      this.mongopenter.setup(this.cbStub);
      tasksStub.calledWith([ 'createDatabases', 'createCollections', 'createDocuments' ]).should.eql(true);
      (typeof(tasksStub.firstCall.args[1])).should.eql('function');
    });
  });

  describe('#createDatabases', function () {
    it('should call the expected task', function () {
      var tasksStub = this.sinon.stub(this.mongopenter, '_tasks');
      this.mongopenter.createDatabases(this.cbStub);
      tasksStub.calledWith([ 'createDatabases' ], this.cbStub).should.eql(true);
    });
  });

  describe('#createCollections', function () {
    it('should call the expected task', function () {
      var tasksStub = this.sinon.stub(this.mongopenter, '_tasks');
      this.mongopenter.createCollections(this.cbStub);
      tasksStub.calledWith([ 'createCollections' ], this.cbStub).should.eql(true);
    });
  });

  describe('#createDocuments', function () {
    it('should call the expected task', function () {
      var tasksStub = this.sinon.stub(this.mongopenter, '_tasks');
      this.mongopenter.createDocuments(this.cbStub);
      tasksStub.calledWith([ 'createDocuments' ], this.cbStub).should.eql(true);
    });
  });

  describe('#_notify', function () {
    beforeEach(function () {
      this.eventHandlers = { notifyTest: [ this.sinon.stub(), this.sinon.stub() ] };
      this.dbStub = { close: this.sinon.stub(), getConnection: this.sinon.stub() };
      this.sinon.stub(this.mongopenter, 'eventHandlers', this.eventHandlers);
      this.sinon.stub(this.mongopenter, 'db', this.dbStub);
    });

    it('should notify listeners and close the connection', function () {
      var expectedError = 'aError';
      var expectedResult = 'aResult';

      this.dbStub.getConnection.callsArgWith(0, null, this.dbStub);
      this.eventHandlers.notifyTest[0].callsArgWith(1, null);
      this.eventHandlers.notifyTest[1].callsArgWith(1, expectedError);
      this.mongopenter._notify('notifyTest', this.cbStub, null, expectedResult);

      this.cbStub.calledWith(expectedError, expectedResult).should.eql(true);
    });

    it('should callback with errors', function () {
      var expectedError = 'aError';
      this.dbStub.getConnection.callsArgWith(0, expectedError);
      this.mongopenter._notify('notifyTest', this.cbStub, null, null);

      this.cbStub.calledWith(expectedError).should.eql(true);
    });

    it('should callback with task errors without notifying if thrown', function (done) {
      var expectedError = 'so soon?';
      this.mongopenter._notify('notifyTest', function (err) {
        err.should.eql(expectedError);
        done();
      }, expectedError, null);
    });
  });

  describe('#_tasks', function () {
    it('should call the given tasks', function () {
      var dbStub = { test: this.sinon.stub(), anotherTest: this.sinon.stub() };
      this.sinon.stub(this.mongopenter, 'db', dbStub);
      dbStub.test.callsArgWith(0);
      dbStub.anotherTest.callsArgWith(0);
      this.mongopenter._tasks([ 'test', 'anotherTest' ], this.cbStub);
      this.cbStub.called.should.eql(true);
    });

    it('should cb with an error if there is no setup config', function (done) {
      this.sinon.stub(this.mongopenter.options, 'setup', undefined);
      this.mongopenter._tasks([], function (err) {
        err.should.eql('No mongopenter setup to execute.');
        done();
      });
    });
  });

});
