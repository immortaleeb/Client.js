var QueryLogger = require('./QueryLogger'),
    fs = require('fs');

var LOGS_DIR = 'logs';

function mkdirIfNotExists(dir) {
  var dirExists = false;
  try {
    var stats = fs.statSync(dir);
    dirExists = stats.isDirectory();
  } catch (e) {
  }

  !dirExists && fs.mkdir(dir);
}

module.exports = {
  init: function (queryIndex) {
    mkdirIfNotExists(LOGS_DIR);
    this.queryIndex = queryIndex;
  },
  query: function (query) {
    this.logger && this.logger.close();
    return this.logger = new QueryLogger(LOGS_DIR, this.queryIndex, query);
  },
  close: function () {
    this.queryIndexer && this.queryIndexer.close();
    this.logger && this.logger.close();
  }
};
