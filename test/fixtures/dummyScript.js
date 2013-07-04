exports = module.exports = DummySetupFile;

function DummySetupFile(mongopenter) {
  mongopenter.on('dummySetup', 'use me for tests');
}
