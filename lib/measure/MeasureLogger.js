var QueryLogger = require('./QueryLogger'),
    path = require('path'),
    fs = require('fs');

var LOGS_DIR = 'logs';

function mkdirIfNotExists(dir) {
  var stats = fs.statSync(dir);
  !stats.isDirectory() && fs.mkdir(dir);
}

module.exports = {
  init: function (queryName) {
    this.logDir = path.join(LOGS_DIR, queryName);
    mkdirIfNotExists(this.logDir);
  },
  query: function (query) {
    this.logger && this.logger.close();
    return this.logger = new QueryLogger(this.logDir, query);
  },
  close: function () {
    this.queryIndexer && this.queryIndexer.close();
    this.logger && this.logger.close();
  }
};
