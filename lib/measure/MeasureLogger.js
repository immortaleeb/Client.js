var QueryLogger = require('./QueryLogger'),
    QueryIndexer = require('./QueryIndexer'),
    path = require('path'),
    fs = require('fs');

var LOGS_DIR = 'logs';

module.exports = {
  init: function (logName) {
    this.logDir = path.join(LOGS_DIR, logName);
    fs.mkdir(this.logDir);

    this.queryIndexer = new QueryIndexer(this.logDir);
  },
  query: function (query) {
    return this.logger = new QueryLogger(this.logDir, this.queryIndexer.add(query), query);
  },
  close: function () {
    this.queryIndexer.close();
    this.logger.close();
  }
};
