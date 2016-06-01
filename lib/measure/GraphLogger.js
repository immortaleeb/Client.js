var csvWriter = require('csv-write-stream'),
    path = require('path'),
    fs = require('fs'),
    TimeMeasure = require('./TimeMeasure');

var MONITOR_UPDATE_RATE = 100;
var CSV_HEADERS = [ 'queryIndex', 'graphIndex', 'action', 'startTimestamp', 'endTimestamp', 'executionTime', 'data' ];
var GRAPH_FILE = 'graphs.csv';

function GraphLogger(logDir, queryIndex, graphIndex, pattern) {
  this.logDir = logDir;
  this.queryIndex = queryIndex;
  this.graphIndex = graphIndex;
  this.pattern = pattern;
  this.timer = new TimeMeasure();
  this._createWriteStream();
}

GraphLogger.prototype._createWriteStream = function () {
  this.writer = csvWriter({ headers: CSV_HEADERS, sendHeaders: false });
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

GraphLogger.prototype.stopRequestTriple = function (bindings) {
  this._stop('request triple', tripleString(this.currentTriple) + ' (' + (bindings && bindings.length) + ')');
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
  this._writeArray([ this.queryIndex, this.graphIndex, action, timeResult.start, timeResult.end, timeResult.delta, data ]);
};

GraphLogger.prototype._writeArray = function (array) {
  this.writer.write(array);
};

GraphLogger.prototype._log = function (message) {
  //console.error(message);
};

GraphLogger.prototype.close = function () {
  // Only write data to the file once everything has been processed
  // this way we don't influence time measurements too much
  this._writeDataToCsvFile();
};

GraphLogger.prototype._writeDataToCsvFile = function () {
  var graphFile = path.join(this.logDir, GRAPH_FILE),
      fileStream = fs.createWriteStream(graphFile, { flags: 'a' });
  this.writer.pipe(fileStream);
  this.writer.end();
};


module.exports = GraphLogger;
