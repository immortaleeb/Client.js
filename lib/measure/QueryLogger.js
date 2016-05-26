var TimeMeasure = require('./TimeMeasure'),
    GraphLogger = require('./GraphLogger'),
    path = require('path'),
    fs = require('fs');

var QUERY_SUMMARY_FILE = "query_summary.json";
var openedGraphLoggers = [];

// Logs things for a single query
function QueryLogger(logDir, queryIndex, query) {
  this.logDir = logDir;
  this.queryLogDir = path.join(logDir, 'query' + queryIndex);
  this.queryIndex = queryIndex;
  this.query = query;

  this.nextGraphIndex = 0;

  this._createQueryDirectory();
}

QueryLogger.prototype._createQueryDirectory = function () {
  fs.mkdir(this.queryLogDir);
};

QueryLogger.prototype.startProcessingQuery = function () {
  this.startTime = new Date();
};

QueryLogger.prototype.stopProcessingQuery = function () {
  var delta = new Date() - this.startTime;
  this._writeSummaryToFile(delta);
};

QueryLogger.prototype._writeSummaryToFile = function (delta) {
  fs.writeFileSync(path.join(this.queryLogDir, QUERY_SUMMARY_FILE), JSON.stringify({
    executionTime: delta
  }, null, 2));
};

QueryLogger.prototype.newGraphLogger = function (pattern) {
  var logger = new GraphLogger(this.queryLogDir, this.nextGraphIndex++, pattern);
  openedGraphLoggers.push(logger);
  return logger;
};

QueryLogger.prototype.close = function () {
  var logger;
  while (logger = openedGraphLoggers.pop()) {
    logger.close();
  }
};

module.exports = QueryLogger;
