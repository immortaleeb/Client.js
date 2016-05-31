var TimeMeasure = require('./TimeMeasure'),
    GraphLogger = require('./GraphLogger'),
    path = require('path'),
    fs = require('fs');

var MONITOR_UPDATE_RATE = 100;
var QUERY_SUMMARY_FILE = "query_summary.json";
var openedGraphLoggers = [];

// Logs things for a single query
function QueryLogger(logDir, queryIndex, query) {
  this.queryLogDir = logDir;
  this.query = query;

  this.nextGraphIndex = 0;
  this.requestCount = 0;
}

QueryLogger.prototype.logRequest = function (url) {
  this.requestCount++;
};

QueryLogger.prototype.startProcessingQuery = function () {
  this.startTime = Date.now();
};

QueryLogger.prototype.stopProcessingQuery = function () {
  var endTime = Date.now();
  if (!this.startTime) return;

  var startTime = this.startTime;
  var delta = endTime - startTime;
  delete this.startTime;

  this._writeSummaryToFile(startTime, endTime, delta);
};

QueryLogger.prototype._writeSummaryToFile = function (startTime, endTime, delta) {
  var summary = {
    startTimestamp: startTime,
    endTimestamp: endTime,
    executionTime: delta,
    requestCount: this.requestCount
  };
  fs.writeFileSync(path.join(this.queryLogDir, QUERY_SUMMARY_FILE), JSON.stringify(summary, null, 2));
  this.requestCount = 0;
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
