/*
 * A TripleGraphIterator uses a TripleGraph and TripleGraphOptimizationTree to find
 * the order of requests that lead to the least amount of requests.
 */

var Iterator = require('../iterators/Iterator'),
    tripleGraph = require('triple-graph'),
    Graph = tripleGraph.Graph,
    OptimizationTree = tripleGraph.OptimizationTree,
    GraphBindingsIterator = tripleGraph.BindingsIterator,
    util = tripleGraph.Util,
    _ = require('underscore'),
    rdf = require('../util/RdfUtil');

// Creates a new TripleGraphIterator
function TripleGraphIterator(pattern, options) {
  Iterator.call(this, options);

  this._options = options;
  this._pattern = pattern;
  this._client = this._options.fragmentsClient;
  var graph = this._graph = new Graph();
  
  pattern.forEach(function(triplePattern) {
    graph.addTriple(triplePattern);
  });

}
Iterator.inherits(TripleGraphIterator);

// Fetches metadata for each triple pattern from the server
TripleGraphIterator.prototype._fetchMetadata = function() {
  this._pattern.forEach(function(triplePattern) {
    var fragment = this._client.getFragmentByPattern(triplePattern);
    fragment.getProperty('metadata', function(metadata) {
      fragment.close();

      var foundMetadata = {
        pageSize: 100, // TODO: fetch pageSize from metadata
        tripleCount: metadata.totalTriples || 0,
        averageSubjectsPerObject: metadata.averageSubjectsPerObject,
        averageObjectsPerSubject: metadata.averageObjectsPerSubject
      };

      graph.updateTripleMetadata(triplePattern, foundMetadata);
    });
  });
};

// Returns the next cheapest request that can be executed based in the information in the TripleGraph
TripleGraphIterator.prototype._getCheapestRequest = function() {
  // TODO: for now we construct a tree explicitly for debugging purposes, but this could actually be done by a simple algorithm
  var tree = new OptimizationTree(this._graph),
      lowestCostNode = tree.getLowestCostNode();

  return lowestCostNode;
};

// Expects bindings in N3 format
TripleGraphIterator.prototype._executeRegularRequest = function(triplePattern, bindings) {
  var options = this._options,
      startIterator = Iterator.single(bindings || {}),
      triplePatternIterator = new TriplePatternIterator(startIterator, triplePattern, options),
      allBindings = [], bindings;

  // Fetch all bindings for the given triple pattern
  while ( (bindings = triplePatternIterator.read()) != null ) {
    // FIXME is bindings an array or a single binding?
    allBindings.push(bindings);
  }

  return allBindings;
};

// Expects bindings in N3 format
TripleGraphIterator.prototype._executeBindingRequest = function(triplePattern, bindingsList) {
  var self = this,
      allBindings = [];

  bindingsList.forEach(function(bindings) {
    bindings = self._executeRegularRequest(triplePattern, bindings);
    allBindings.push(bindings);
  });

  return _.flatten(allBindings);
};

// Executes a triple pattern request given a request node
TripleGraphIterator.prototype._executeRequest = function(requestNode) {
  var bindings, bindingsList, variable,
      graph = this._graph,
      triplePattern = requestNode.triple,
      roles = extractRolesFromTriplePattern(triplePattern), 
      variables = extractVariablesFromTriplePattern(triplePattern);

  // TODO: clean this up
  switch(requestNode.type) {
    // Regular request: performs a hash join
    case 'request':
      // Request the bindings
      bindings = this._executeRegularRequest(requestNode.triple);
      // Convert to graph bindings
      bindings = N3BindingsToGraphBindings(bindings, roles);
      // Update the graph
      graph.updateTripleBindings(triple, bindings);
      break;

    // Binding request: performs a nested loop join
    case 'bindingRequest':
      // Create a bindingsList in N3 format
      variable = requestNode.variable;
      bindingsList = requestNode.bindings.map(function(binding) { return createObject(variable, binding) });

      // Request the bindings using the already given bindings
      bindings = this._executeBindingRequest(triplePattern, bindingsList);
      // Convert bindings back to graph format
      bindings = N3BindingsToGraphBindings(bindings, roles);
      // Update the graph
      graph.updateTripleBindings(triplePattern, bindings);
      break;

    default: throw new Error('Unsupported request type found: ' + requestNode.type);
  }
};

TripleGraphIterator.prototype._read = function() {
  var cheapestRequest;

  // Fetch the metadata if it hasn't been fetched yet
  if (!this._isMetadataFetched) {
    this._fetchMetadata();
    this._isMetadataFetched = true;
  }
 
  // Get the next cheapest request untill there is none to be found
  while (cheapestRequest = this._getCheapestRequest()) {
    this._executeRequest(cheapestRequest);
  }

  // No cheapest request found, this means we've requested all triple patterns: return results
  // Create a bindings iterator
  if (!this._iterator) this._iterator = new GraphBindingsIterator(this._graph);

  // Return all bindings for all variables in the graph one at a time
  return this._iterator.read();
};

function createObject() {
  var object = {};
  for (var i = 0; i < arguments.length; i+=2) {
    var key = arguments[i],
        val = arguments[i+1];
    object[key] = val;
  }
  return object;
}

module.exports = TripleGraphIterator;

