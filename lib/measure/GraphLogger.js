var csvWriter = require('csv-write-stream'),
    path = require('path'),
    fs = require('fs'),
    TimeMeasure = require('./TimeMeasure'),
    Monitor = require('./Monitor');

var MONITOR_UPDATE_RATE = 10;

function GraphLogger(logDir, graphIndex, pattern) {
  this.logDir = logDir;
  this.graphIndex = graphIndex;
  this.pattern = pattern;

  this.timer = new TimeMeasure();
  this.monitor = new Monitor(MONITOR_UPDATE_RATE);

  this._createFilesAndStreams();
}

GraphLogger.prototype._createFilesAndStreams = function () {
  this.graphFile = path.join(this.logDir, 'graph' + this.graphIndex + '.csv');
  this.writer = csvWriter();
  this.writer.pipe(fs.createWriteStream(this.graphFile));
};

GraphLogger.prototype.startBuildingGraph = function () {
  this._start('building graph');
};

GraphLogger.prototype.stopBuildingGraph = function () {
  this._stop('building graph');
};

GraphLogger.prototype.startFillingGraph = function () {
  this._start('filling graph');
};

GraphLogger.prototype.stopFillingGraph = function () {
  this._stop('filling graph');
};

GraphLogger.prototype.startFetchingMetadata = function () {
  this._start('fetching metadata');
};

GraphLogger.prototype.stopFetchingMetadata = function () {
  this._stop('fetching metadata');
};

GraphLogger.prototype.startCalculatingCheapestRequest = function () {
  this._start('calculating cheapest request');
};

GraphLogger.prototype.stopCalculatingCheapestRequest = function (request) {
  this._stop('calculating cheapest request', request && { type: request.isBound ? "binding" : "direct", triple: request.triple, cost: request.cost });
  this.currentRequest = request;
  this.currentTriple = request && request.triple;
};

GraphLogger.prototype.startRequestTriple = function () {
  this._start('request triple');
};

GraphLogger.prototype.stopRequestTriple = function () {
  this._stop('request triple', this.currentTriple);
};

GraphLogger.prototype.startPropagatingBindings = function () {
  this._start('propagating bindings');
};

GraphLogger.prototype.stopPropagatingBindings = function () {
  this._stop('propagating bindings', this.currentTriple);
};

GraphLogger.prototype.startJoiningResults = function () {
  this._start('joining results');
};

GraphLogger.prototype.stopJoiningResults = function () {
  this._stop('joining results');
};

GraphLogger.prototype._start = function (action) {
  this.monitor.start(action);
  this.timer.start(action);
};

GraphLogger.prototype._stop = function (action, data) {
  var delta = this.timer.stop(action);
  var loadSummary = this.monitor.stop(action);
  this._writeRow(action, data, delta, loadSummary);
};

GraphLogger.prototype._writeRow = function (action, data, delta, loadSummary) {
  this.writer.write({
    action: action,
    executionTime: delta,
    minCpu: loadSummary.cpu.min,
    avgCpu: loadSummary.cpu.avg,
    maxCpu: loadSummary.cpu.max,
    minMem: loadSummary.mem.min,
    avgMem: loadSummary.mem.avg,
    maxMem: loadSummary.mem.max,
    data: JSON.stringify(data)
  });
};

GraphLogger.prototype.close = function () {
  this.writer.end();
};

module.exports = GraphLogger;
