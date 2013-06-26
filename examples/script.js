exports = module.exports = MongoCallbacks;

function MongoCallbacks(mongopenter) {
  this.mongopenter = mongopenter;
  mongopenter.on('setupComplete', this.onSetupComplete);
}

MongoCallbacks.prototype.onSetupComplete = function (dbConnection, cb) {
  // The dbConnection is a connection to the urls and will be automatically closed once all scripts are done
  var db = dbConnection.db('anotherDatabaseName');
  db.collection('yetAnotherCollection', function (err, collection) {
    if (err) cb(err);
    // Do whatever you need to check if stuff has been run before
    collection.insert({ oh: 'what a lovely tea party' }, cb);
  });
};
