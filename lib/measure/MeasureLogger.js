var fs = require('fs'),
    path = require('path');

var LOG_DIR = 'logs',
    QUERY_SUMMARY_FILE = 'query_summary.csv';

function MeasureLogger() {
  this.requestCount = 0;
  this.totalResponseSize = 0;
}

MeasureLogger.prototype.init = function (queryIndex) {
  this.queryIndex = queryIndex || 0;
};

MeasureLogger.prototype.logRequest = function (url) {
  this.requestCount++;
};

MeasureLogger.prototype.logResponseSize = function (url, responseSize) {
  (this.requestCount > 1) && (this.totalResponseSize += responseSize || 0);
};

MeasureLogger.prototype.startProcessingQuery = function () {
  this.startTime = Date.now();
};

MeasureLogger.prototype.stopProcessingQuery = function () {
  var endTime = Date.now(),
      startTime = this.startTime,
      delta = endTime - startTime;

  var summary = {
    startTimestamp: startTime,
    endTimestamp: endTime,
    executionTime: delta,
    requestCount: this.requestCount-1, // don't count the initial request
    totalResponseSize: this.totalResponseSize
  };

  this._writeToFile(summary);
  this.requestCount = 0;
};

MeasureLogger.prototype._writeToFile = function (summary) {
  try { fs.mkdirSync(LOG_DIR); } catch (e) { }

  var querySummaryFile = path.join(LOG_DIR, QUERY_SUMMARY_FILE);
  fs.appendFileSync(querySummaryFile, this._summaryToRow(summary) + '\n');
};

MeasureLogger.prototype._summaryToRow = function(summary) {
  return [
    this.queryIndex,
    summary.startTimestamp, summary.endTimestamp, summary.executionTime,
    summary.requestCount, summary.totalResponseSize, false
  ].join(',');
};

MeasureLogger.instance = new MeasureLogger();
module.exports = MeasureLogger.instance;
