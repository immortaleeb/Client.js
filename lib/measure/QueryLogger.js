var TimeMeasure = require('./TimeMeasure'),
    GraphLogger = require('./GraphLogger'),
    path = require('path'),
    fs = require('fs'),
    csvWriter = require('csv-write-stream');

var MONITOR_UPDATE_RATE = 100;
var QUERY_SUMMARY_FILE = "query_summary.csv";
var openedGraphLoggers = [];

// Logs things for a single query
function QueryLogger(logDir, queryIndex, query) {
  this.logDir = logDir;
  this.queryIndex = queryIndex || 0;
  this.query = query;

  this.nextGraphIndex = 0;
  this.requestCount = 0;
  this.totalResponseSize = 0;
}

QueryLogger.prototype.logRequest = function (url) {
  this.requestCount++;
};

QueryLogger.prototype.logResponseSize = function (url, responseSize) {
  this.requestCount > 0 && (this.totalResponseSize += responseSize);
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
    queryIndex: this.queryIndex,
    startTimestamp: startTime,
    endTimestamp: endTime,
    executionTime: delta,
    requestCount: this.requestCount,
    totalResponseSize: this.totalResponseSize,
    timedOut: false
  };

  var querySummaryFile = path.join(this.logDir, QUERY_SUMMARY_FILE),
      fileStream = fs.createWriteStream(querySummaryFile, { flags: 'a'}),
      writer = csvWriter({ sendHeaders: false });

  writer.pipe(fileStream);
  writer.end(summary);
  this.requestCount = 0;
};

QueryLogger.prototype.newGraphLogger = function (pattern) {
  var logger = new GraphLogger(this.logDir, this.queryIndex, this.nextGraphIndex++, pattern);
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
