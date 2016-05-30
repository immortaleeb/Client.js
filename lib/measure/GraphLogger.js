var csvWriter = require('csv-write-stream'),
    path = require('path'),
    fs = require('fs'),
    TimeMeasure = require('./TimeMeasure');

var MONITOR_UPDATE_RATE = 100;

function GraphLogger(logDir, graphIndex, pattern) {
  this.logDir = logDir;
  this.graphIndex = graphIndex;
  this.pattern = pattern;
  this.timer = new TimeMeasure();

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

GraphLogger.prototype.stopBuildingGraph = function (graph) {
  this._stop('building graph');
  this.graph = graph;
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

function tripleString(triple) {
  return triple.subject + ' ' + triple.predicate + ' ' + triple.object;
}

GraphLogger.prototype.stopCalculatingCheapestRequest = function (request) {
  this._stop('calculating cheapest request', request &&
      ((request.isBound ? "BINDING" : "DIRECT") + "(" + request.cost + ") : ") +
      tripleString(request.triple));

  if (request)
    this._log('cheap request (' + request.cost + ') ' + request.isBound + ' ' + tripleString(request.triple));

  this.currentRequest = request;
  this.currentTriple = request && request.triple;
};

GraphLogger.prototype.startRequestTriple = function () {
  this._start('request triple');
  this._log('request ' + tripleString(this.currentTriple));
};

GraphLogger.prototype.stopRequestTriple = function () {
  this._stop('request triple', tripleString(this.currentTriple));
  this._log('response ' + tripleString(this.currentTriple));
};

GraphLogger.prototype.startPropagatingBindings = function () {
  this._start('propagating bindings');
};

GraphLogger.prototype.stopPropagatingBindings = function () {
  this._stop('propagating bindings', tripleString(this.currentTriple));
  this._log(this.graph.nodes.map(function (node) {
    return {
      variable: node.variable,
      bindings: node.bindings
    };
  }));
};

GraphLogger.prototype.startJoiningResults = function () {
  this._start('joining results');
};

GraphLogger.prototype.stopJoiningResults = function () {
  this._stop('joining results');
};

GraphLogger.prototype._start = function (action) {
  this._log('START ' + action);
  this.timer.start(action);
};

GraphLogger.prototype._stop = function (action, data) {
  var timeResult = this.timer.stop(action);
  this._log('STOP ' + action);
  timeResult && this._writeRow(action, data, timeResult);
};

GraphLogger.prototype._writeRow = function (action, data, timeResult) {
  this.writer.write({
    action: action,
    startTimestamp: timeResult.start,
    endTimestamp: timeResult.end,
    executionTime: timeResult.delta,
    data: data
  });
};

GraphLogger.prototype._log = function (message) {
  //console.error(message);
};

GraphLogger.prototype.close = function () {
  this.writer.end();
};

module.exports = GraphLogger;
