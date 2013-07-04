var async = require('async');
var Db = require('./db');
var path = require('path');
var fs = require('fs');

exports = module.exports = Mongopenter;

function Mongopenter(urls, opts) {
  if (urls || opts) {
    this.init(urls, opts);
  }
}

Mongopenter.prototype.init = function (urls, opts) {
  this.eventHandlers = {};
  this.options = {
    urls: urls || process.env.MONGODB_URL || 'mongodb://localhost/admin',
    setupFile: this.resolveFile(opts.setupFile || 'mongopenter.json'),
    setup: {}
  };

  if (fs.existsSync(this.options.setupFile)) {
    this.options.setup = JSON.parse(fs.readFileSync(this.options.setupFile));
    this._loadScripts();
  }

  this.db = new Db(this, this.options);
};

Mongopenter.prototype._loadScripts = function () {
  var setup = this.options.setup;
  var self = this;
  if (setup.scripts) {
    setup.scripts.forEach(function (script) {
      var ScriptConstructor = require(self.resolveFile(script));
      new ScriptConstructor(self);
    });
  }
};

Mongopenter.prototype.on = function (event, fn) {
  (this.eventHandlers[event] = this.eventHandlers[event] || []).push(fn);
};

Mongopenter.prototype.setup = function (cb) {
  this._tasks([ 'createDatabases', 'createCollections', 'createDocuments' ], this._notify.bind(this, 'setupComplete', cb));
};

Mongopenter.prototype.createDatabases = function (cb) {
  this._tasks([ 'createDatabases' ], cb);
};

Mongopenter.prototype.createCollections = function (cb) {
  this._tasks([ 'createCollections' ], cb);
};

Mongopenter.prototype.createDocuments = function (cb) {
  this._tasks([ 'createDocuments' ], cb);
};

Mongopenter.prototype._notify = function (event, cb, taskErr, taskResult) {
  var self = this;
  var eventHandlers = self.eventHandlers[event];
  if (!taskErr && eventHandlers && eventHandlers.length > 0) {
    this.db.getConnection(function (err, db) {
      if (err) return cb(err);

      var jobs = eventHandlers.map(function (eventHandler) {
        return eventHandler.bind(this, db);
      });
      async.series(jobs, function (err) {
        db.close();
        cb(err, taskResult);
      });
    });
  } else {
    process.nextTick(cb.bind(this, taskErr, taskResult));
  }
};

Mongopenter.prototype._tasks = function (tasks, cb) {
  var self = this;
  if (self.options.setup && self.options.urls) {
    var jobs = tasks.map(function (task) {
      return self.db[task].bind(self.db);
    });
    async.series(jobs, cb);
  } else {
    process.nextTick(cb.bind(self, 'No mongopenter setup to execute.'));
  }
};

Mongopenter.prototype.resolveFile = function (filePath) {
  var dirname = this.options && this.options.setupFile ? path.dirname(this.options.setupFile) : process.cwd();
  return path.resolve(dirname, filePath);
};
