var fs = require('fs'),
    path = require('path');

var LOG_DIR = 'logs',
    QUERY_SUMMARY_FILE = 'query_summary.json';

function MeasureLogger() {
  this.requestCount = 0;
}

MeasureLogger.prototype.init = function (queryName) {
  this.queryName = queryName;
};

MeasureLogger.prototype.logRequest = function (url) {
  this.requestCount++;
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
    requestCount: this.requestCount
  };

  this._writeToFile(summary);
  this.requestCount = 0;
};

MeasureLogger.prototype._writeToFile = function (summary) {
  try { fs.mkdirSync(LOG_DIR); } catch (e) { }
  try { fs.mkdirSync(path.join(LOG_DIR, this.queryName)); } catch (e) { }
  fs.writeFileSync(path.join(LOG_DIR, this.queryName, QUERY_SUMMARY_FILE), JSON.stringify(summary, null, 2));
};

MeasureLogger.instance = new MeasureLogger();
module.exports = MeasureLogger.instance;
