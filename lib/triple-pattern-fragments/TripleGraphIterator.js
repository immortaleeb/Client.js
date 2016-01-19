/*
 * A TripleGraphIterator uses a TripleGraph and TripleGraphOptimizationTree to find
 * the order of requests that lead to the least amount of requests.
 */

var Iterator = require('../iterators/Iterator'),
    TriplePatternIterator = require('./TriplePatternIterator'),
    tripleGraph = require('triple-graph'),
    Graph = tripleGraph.Graph,
    OptimizationTree = tripleGraph.OptimizationTree,
    GraphBindingsIterator = tripleGraph.BindingsIterator,
    util = tripleGraph.Util,
    _ = require('underscore'),
    rdf = require('../util/RdfUtil'),
    collectors = require('../iterators/collectors');

// Creates a new TripleGraphIterator
function TripleGraphIterator(pattern, options) {
  Iterator.call(this, options);

  this._options = options;
  this._pattern = pattern;
  this._client = this._options.fragmentsClient;
  var graph = this._graph = new Graph();

  pattern.forEach(function (triplePattern) {
    graph.addTriple(triplePattern);
  });

}
Iterator.inherits(TripleGraphIterator);

// Fetches metadata for each triple pattern from the server
TripleGraphIterator.prototype._fetchMetadata = function (callback) {
  var client = this._client,
      graph = this._graph,
      patternCount = this._pattern.length,
      fetchedMetadataCount = 0;

  if (this._fetchingMetadata) return;
  this._fetchingMetadata = true;

  this._pattern.forEach(function (triplePattern) {
    var fragment = client.getFragmentByPattern(triplePattern);
    fragment.getProperty('metadata', function (metadata) {
      fragment.close();

      var foundMetadata = {
        pageSize: 100, // TODO: fetch pageSize from metadata
        tripleCount: metadata.totalTriples || 0
      };

      if (metadata.averageSubjectsPerObject) foundMetadata.averageSubjectsPerObject =  metadata.averageSubjectsPerObject;
      if (metadata.averageObjectsPerSubject) foundMetadata.averageObjectsPerSubject = metadata.averageObjectsPerSubject;
      console.error('found metadata', triplePattern, foundMetadata);

      graph.updateTripleMetadata(triplePattern, foundMetadata);

      if (++fetchedMetadataCount == patternCount)
        callback();
    });
  });
};

// Returns the next cheapest request that can be executed based in the information in the TripleGraph
TripleGraphIterator.prototype._getCheapestRequest = function () {
  // TODO: for now we construct a tree explicitly for debugging purposes, but this could actually be done by a simple algorithm
  var tree = new OptimizationTree(this._graph),
      lowestCostNode = tree.getLowestCostNode();

  return lowestCostNode;
};

// Expects bindings in N3 format
TripleGraphIterator.prototype._executeRegularRequest = function (triplePattern, bindings, callback) {
  console.error('regular request', triplePattern);

  // Resolve arguments
  if (typeof bindings === 'function') callback = bindings, bindings = undefined;

  var options = this._options,
      startIterator = Iterator.single(bindings || {}),
      triplePatternIterator = new TriplePatternIterator(startIterator, triplePattern, options);

  collectors.arrayCollector(triplePatternIterator, callback);
};

// Expects bindings in N3 format
TripleGraphIterator.prototype._executeBindingRequest = function (triplePattern, bindingsList, callback) {
  console.error('binding request', triplePattern);
  var self = this,
      allBindings = [],
      executedRequests = 0;

  bindingsList.forEach(function (bindings) {
    // Execute a request for each possible given binding
    self._executeRegularRequest(triplePattern, bindings, function (requestedBindings) {
      // Add the requested bindings to the bindings array
      allBindings.push(requestedBindings);
      executedRequests++;

      // If this was the last request, return the results using the callback
      if (executedRequests == bindingsList.length) {
        callback(_.flatten(allBindings));
      }
    });
  });
};

// Executes a triple pattern request given a request node
TripleGraphIterator.prototype._executeRequest = function (requestNode, callback) {
  var bindingsList, variable,
      graph = this._graph,
      triplePattern = requestNode.triple,
      roles = util.extractRolesFromTriplePattern(triplePattern),
      variables = util.extractVariablesFromTriplePattern(triplePattern);

  // TODO: clean this up
  switch (requestNode.type) {
  // Regular request: performs a hash join
  case 'request':
    // Request the bindings
    this._executeRegularRequest(requestNode.triple, function (bindings) {
      console.error('bindings', bindings);
      // Convert to graph bindings
      bindings = util.N3BindingsToGraphBindings(bindings, roles);
      // Update the graph
      graph.updateTripleBindings(triplePattern, bindings);
      callback();
    });
    break;

  // Binding request: performs a nested loop join
  case 'bindingRequest':
    // Create a bindingsList in N3 format
    variable = requestNode.variable;
    bindingsList = requestNode.bindings.map(function (binding) { return createObject(variable, binding); });

    // Request the bindings using the already given bindings
    this._executeBindingRequest(triplePattern, bindingsList, function (bindings) {
      console.error('bindings', bindings);
      // Convert bindings back to graph format
      bindings = util.N3BindingsToGraphBindings(bindings, roles);
      // Update the graph
      graph.updateTripleBindings(triplePattern, bindings);
      callback();
    });
    break;

  default:
    throw new Error('Unsupported request type found: ' + requestNode.type);
  }
};

// Fetches cheaps requests one at a time and executes each in turn until no requests are left
TripleGraphIterator.prototype._executeAllCheapRequests = function (callback) {
  var cheapestRequest = this._getCheapestRequest(),
      self = this;

  console.error('cheapest request', cheapestRequest);
  if (cheapestRequest)
    this._executeRequest(cheapestRequest, function () {
      console.error('executed request');
      self._executeAllCheapRequests(callback);
    });
  else
    callback();
};

TripleGraphIterator.prototype._read = function () {
  console.error('_read() called');
  var self = this,
      bindings;

  if (!this._iterator) {
    // Fetch inital the metadata first
    this._fetchMetadata(function () {
      // Loop over all requests and execute them in turn,
      // updating the graph after each request
      self._executeAllCheapRequests(function () {
        // Create an iterator that iterates over all bindings in the graph
        self._iterator = new GraphBindingsIterator(self._graph);

        // Read the bindings
        bindings = self._iterator.read();
        bindings && self._push(bindings);
      });
    });
  } else {
    // Return all bindings for all variables in the graph one at a time
    bindings = this._iterator.read();
    if (bindings)
      this._push(bindings);
    else
      this._end();
  }
};

function createObject() {
  var object = {};
  for (var i = 0; i < arguments.length; i += 2) {
    var key = arguments[i],
        val = arguments[i + 1];
    object[key] = val;
  }
  return object;
}

module.exports = TripleGraphIterator;

