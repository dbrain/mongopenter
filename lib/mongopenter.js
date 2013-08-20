var async = require('async');
var Db = require('./db');
var path = require('path');
var fs = require('fs');

exports.Mongopenter = Mongopenter;

function Mongopenter(urls, opts) {
  if (urls || opts) {
    this.init(urls, opts);
  }
}

/**
 * Work around for whacky mongo driver connection syntax
 * It wants mongodb://username:auth@host:port,host:port,host:port,host:port/db
 */
Mongopenter.getConnectionStringFromUrlArray = function (urls) {
  if (typeof(urls) === 'object') {
    return urls.reduce(function (connectionStringParts, url, index) {
      var endOfAuthIndex, dbIndex;
      if (index === 0) {
        // First position, we want mongodb://user:auth@host:port
        dbIndex = url.lastIndexOf('/');
        connectionStringParts.push(url.substring(0, dbIndex > 9 ? dbIndex : undefined));
      } else if (urls.length - 1 === index) {
        // End position, we want host:port/db
        url = url.replace('mongodb://', '');
        endOfAuthIndex = url.lastIndexOf('@');
        connectionStringParts.push(url.substring(endOfAuthIndex > 0 ? endOfAuthIndex + 1 : 0));
      } else {
        // Middle position, we want host:port
        url = url.replace('mongodb://', '');
        endOfAuthIndex = url.lastIndexOf('@');
        dbIndex = url.lastIndexOf('/');
        connectionStringParts.push(url.substring(endOfAuthIndex > 0 ? endOfAuthIndex + 1 : 0, dbIndex > 0 ? dbIndex : undefined));
      }
      return connectionStringParts;
    }, []).join(',');
  }
  return urls;
};

Mongopenter.prototype.init = function (urls, opts) {
  this.eventHandlers = {};
  this.options = {
    urls: urls || process.env.MONGODB_URL || 'mongodb://localhost/admin',
    setupFile: this.resolveFile(opts.setupFile || 'mongopenter.json'),
    shards: opts.shards || '',
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
  this._tasks([ 'createShards', 'createDatabases', 'createCollections', 'createDocuments', 'addShards' ], this._notify.bind(this, 'setupComplete', cb));
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
