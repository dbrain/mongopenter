var cli = require('../lib/cli');
var Mongopenter = require('../lib/mongopenter').Mongopenter;
var sinon = require('sinon');
var should = require('should');
var bag = require('bagofcli');

describe('CLI', function () {
  beforeEach(function() {
    this.sinon = sinon.sandbox.create();
    this.consoleLogStub = this.sinon.stub(console, 'log');
  });

  afterEach(function() {
    this.sinon.restore();
  });

  describe('#exec', function () {
    it('should load the correct actions', function () {
      var bagStub = this.sinon.stub(bag, 'command');
      cli.exec();
      bagStub.called.should.eql(true);
    });
  });

  describe('#_execCommand', function () {
    it('should execute the given function and log', function () {
      var createDatabasesStub = this.sinon.stub(Mongopenter.prototype, 'createDatabases');
      createDatabasesStub.callsArgWith(0);
      cli._execCommand('createDatabases', { urls: 'blah', parent: { setupFile: 'rah' }});
      this.consoleLogStub.calledWith('Mongopenter: Done.').should.eql(true);
    });
  });
});
