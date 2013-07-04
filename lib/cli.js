var bag = require('bagofcli');
var Mongopenter = require('./mongopenter').Mongopenter;

exports.exec = exec;
exports._execCommand = _execCommand;

function _execCommand(fn, args) {
  var mongopenter = new Mongopenter(args.urls, { setupFile: args.parent.setupFile });
  mongopenter[fn](function (err, results) {
    console.log('Mongopenter: Done.');
  });
}

function exec() {
  var actions = {
    commands: {
      setup: { action: _execCommand.bind(this, 'setup') },
      "setup-db": { action: _execCommand.bind(this, 'createDatabases') },
      "setup-collection": { action: _execCommand.bind(this, 'createCollections') },
      "setup-doc": { action: _execCommand.bind(this, 'createDocuments') }
    }
  };
  bag.command(__dirname, actions);
}
