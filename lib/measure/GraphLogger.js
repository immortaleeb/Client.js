var fs = require('fs'),
    path = require('path'),
    util = require('util'),
    TimeMeasure = require('./TimeMeasure');

function GraphLogger(logDir, graphIndex, pattern) {
  this.logDir = logDir;
  this.graphIndex = graphIndex;
  this.pattern = pattern;
  this.graphFile = path.join(logDir, 'graph' + graphIndex + '.csv');
  this.stream = fs.createWriteStream(this.graphFile);
  this.timer = new TimeMeasure();
}

GraphLogger.prototype.startBuildingGraph = function () {
  this._start('building graph for pattern ' + util.inspect(this.pattern));
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
  this._start('calculing cheapest request');
};

GraphLogger.prototype.stopCalculatingCheapestRequest = function (request) {
  this._stop('calculing cheapest request: ' + util.inspect(request && request.triple));

  if (!request) return;

  this.currentRequest = request;
  this.currentTriple = request.triple;
};

GraphLogger.prototype.startRequestTriple = function () {
  this._start('request triple ' + util.inspect(this.currentTriple));
};

GraphLogger.prototype.stopRequestTriple = function () {
  this._stop('request triple ' + util.inspect(this.currentTriple));
};

GraphLogger.prototype.startPropagatingBindings = function () {
  this._start('propagating bindings for triple ' + util.inspect(this.currentTriple));
};

GraphLogger.prototype.stopPropagatingBindings = function () {
  this._stop('propagating bindings for triple ' + util.inspect(this.currentTriple));
};

GraphLogger.prototype.startJoiningResults = function () {
  this._start('joining results');
};

GraphLogger.prototype.stopJoiningResults = function () {
  this._stop('joining results');
};

GraphLogger.prototype._start = function (message) {
  this._println("START " + message);
};

GraphLogger.prototype._stop = function (message) {
  this._println("END " + message);
};

GraphLogger.prototype._println = function (message) {
  this._print(message + "\n");
};

GraphLogger.prototype._print = function (message) {
  this.stream.write(message);
};

module.exports = GraphLogger;
