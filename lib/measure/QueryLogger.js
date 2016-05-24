var TimeMeasure = require('./TimeMeasure'),
    GraphLogger = require('./GraphLogger'),
    path = require('path'),
    fs = require('fs');

var QUERY_SUMMARY_FILE = "query_summary.json";

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
  fs.writeFileSync(path.join(this.logDir, QUERY_SUMMARY_FILE), JSON.stringify({
    index: this.queryIndex,
    query: this.query,
    executionTime: delta
  }, null, '  '));
};

QueryLogger.prototype.newGraphLogger = function (pattern) {
  return new GraphLogger(this.queryLogDir, this.nextGraphIndex++, pattern);
};

module.exports = QueryLogger;
