var TimeMeasure = require('./TimeMeasure'),
    GraphLogger = require('./GraphLogger'),
    Monitor = require('load-monitor'),
    path = require('path'),
    fs = require('fs');

var MONITOR_UPDATE_RATE = 100;
var QUERY_SUMMARY_FILE = "query_summary.json";
var openedGraphLoggers = [];

// Logs things for a single query
function QueryLogger(logDir, queryIndex, query) {
  this.logDir = logDir;
  this.queryLogDir = path.join(logDir, 'query' + queryIndex);
  this.queryIndex = queryIndex;
  this.query = query;

  this.nextGraphIndex = 0;
  this.monitor = new Monitor(MONITOR_UPDATE_RATE);

  this._createQueryDirectory();
}

QueryLogger.prototype._createQueryDirectory = function () {
  fs.mkdir(this.queryLogDir);
};

QueryLogger.prototype.startProcessingQuery = function () {
  this.monitor.start();
  this.startTime = new Date();
};

QueryLogger.prototype.stopProcessingQuery = function () {
  var delta = new Date() - this.startTime;
  var loadSummary = this.monitor.stop();
  this._writeSummaryToFile(delta, loadSummary);
};

QueryLogger.prototype._writeSummaryToFile = function (delta, loadSummary) {
  var summary = loadSummary || {};
  summary.executionTime = delta;
  fs.writeFileSync(path.join(this.queryLogDir, QUERY_SUMMARY_FILE), JSON.stringify(summary, null, 2));
};

QueryLogger.prototype.newGraphLogger = function (pattern) {
  var logger = new GraphLogger(this.queryLogDir, this.nextGraphIndex++, pattern);
  openedGraphLoggers.push(logger);
  return logger;
};

QueryLogger.prototype.close = function () {
  this.monitor.close();

  var logger;
  while (logger = openedGraphLoggers.pop()) {
    logger.close();
  }
};

module.exports = QueryLogger;
