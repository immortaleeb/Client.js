var _ = require('underscore');

var Iterator = require('../iterators/Iterator'),
    TriplePatternIterator = require('./TriplePatternIterator'),
    tripleGraph = require('triple-graph'),
    Graph = tripleGraph.Graph,
    CheapestRequestIterator = tripleGraph.CheapestRequestIterator,
    GraphBindingsIterator = tripleGraph.GraphBindingsIterator,
    Util = tripleGraph.Util,
    collectors = require('../iterators/collectors');

// Maps a bindingsList to a single object that maps each variable to an array of values that bind that variable
function mapBindings(bindingsList, variables) {
  var object = {};
  // Map each variable to an empty array
  variables.forEach(function (variable) {
    object[variable] = [];
  });

  // Go over all bindings and push them at the end of each array
  bindingsList.forEach(function (bindings) {
    variables.forEach(function (variable) {
      var binding = bindings[variable];
      binding && object[variable].push(binding);
    });
  });

  return object;
}

function TripleGraphIterator(parent, pattern, options) {
  // Empty patterns return no bindings
  if (!pattern || !pattern.length)
    return new Iterator.passthrough(parent, options);
  // A single triple can be solved quicker by a triple pattern iterator
  if (pattern.length === 1)
    return new TriplePatternIterator(parent, pattern[0], options);
  Iterator.call(this, options);

  this._options = options;
  this._pattern = pattern;
  this._client = this._options.fragmentsClient;
  this._parent = parent;

  // Create a graph and set it up
  var graph = this._graph = new Graph();
  pattern.forEach(function (triplePattern) {
    graph.addTriple(triplePattern);
  });

  // Create a request iterator
  this._requestIterator = new CheapestRequestIterator(this._graph);
}
Iterator.inherits(TripleGraphIterator);

// Fills the graph with bindings from the given parent iterator
TripleGraphIterator.prototype._fillGraph = function (parent, callback) {
  if (!parent) return callback();

  var self = this;

  // for TripleGraphIterators, it better to get the bindings from the graph itself
  if (parent instanceof TripleGraphIterator) {
    parent.forceResolveGraph(function (parentGraph) {

      // Add the bindings for all variables from the parent graph in the child graph
      this._graph.nodes.forEach(function (node) {
        if (node.isVariable) {
          var parentNode = parentGraph.findNodeByValue(node.variable);
          // The parent node should have bindings
          if (!parentNode.hasBindings)
            throw new Error("Parent graph doesn't have any binings for node '" + node.variable + "'");
          // Add these bindings to the corresponding node in the child graph
          node.addBindings(parentNode.bindings);
        }
      });

      callback();
    });
  } // Otherwise we need to collect them by reading the iterator
  else {
    // Collect all bindings
    collectors.arrayCollector(parent, function (bindingsList) {
      // Get all bindings for all variables in the current graph
      var nodes = self._graph.nodes.filter(function (node) { return node.isVariable; }),
          variables = nodes.map(function (node) { return node.variable; }),
          bindingsMap = mapBindings(bindingsList, variables);

      // Add all matching bindings to their respective nodes
      nodes.forEach(function (node) {
        node.addBindings(bindingsMap[node.variable]);
      });

      callback();
    });
  }
};

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

      if (metadata.subjectsPerObjectCount) foundMetadata.subjectsPerObjectCount =  metadata.subjectsPerObjectCount;
      if (metadata.objectsPerSubjectCount) foundMetadata.objectsPerSubjectCount = metadata.objectsPerSubjectCount;

      graph.updateTripleMetadata(triplePattern, foundMetadata);

      if (++fetchedMetadataCount == patternCount)
        callback();
    });
  });
};

// Fetches all triples matching the given pattern in a single array and returns it via the callback
TripleGraphIterator.prototype._executeDirectRequest = function (triplePattern, bindings, callback) {
  // Parse arguments
  if (typeof bindings === 'function') {
    callback = bindings;
    bindings = undefined;
  }

  // Create an iterator over all triples that match the triple pattern
  var options = this._options,
      startIterator = Iterator.single(bindings || {}),
      triplePatternIterator = new TriplePatternIterator(startIterator, triplePattern, options);

  // Collect all the triples in a single array
  collectors.arrayCollector(triplePatternIterator, callback);
};

// Fetches all triples that match the given triple pattern and bindings and returns them as an array via the callback
TripleGraphIterator.prototype._executeBindingRequest = function (triplePattern, bindingsList, callback) {
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

// Executes a single request and returns the fetched bindings in the callback
TripleGraphIterator.prototype._executeRequest = function (request, callback) {
  console.error('REQUEST', request.triple, request.isBound ? 'bound': 'direct');
  var graph = this._graph,
      bindingsCallback = function (bindings) {
        console.error('RESPONSE', request.triple, bindings);
        // Update the bindings in the graph
        graph.updateTripleBindings(request.triple, bindings);
        callback();
      };

  if (request.isBound) {
    this._executeBindingRequest(
        request.triple,
        request.boundNode.bindings.map(function (binding) {
          return Util.createObject(request.boundNode.variable, binding);
        }),
        bindingsCallback
    );
  } else {
    this._executeDirectRequest(request.triple, bindingsCallback);
  }
};

// Executes all cheap requests and updates the bindings in the graph
TripleGraphIterator.prototype._executeAllCheapRequests = function (callback) {
  var it = this._requestIterator,
      self = this;

  if (!it.hasNext()) return callback();

  this._executeRequest(it.next(), function () {
    self._executeAllCheapRequests(callback);
  });
};

// Forces all triple patterns in the graph to be bound
TripleGraphIterator.prototype.forceResolveGraph = function (callback) {
  if (this._graphResolved) return callback(this._graph);
  var self = this;

  // Fill the graph with bindings from the parent iterator first
  this._fillGraph(this._parent, function () {
    // Fetch the initial metadata
    self._fetchMetadata(function () {
      // Loop over all requests and execute them in turn,
      // updating the graph after each request
      self._executeAllCheapRequests(function () {
        // Return the graph
        callback(self._graph);
      });
    });
  });
};

TripleGraphIterator.prototype._read = function () {
  var self = this,
      bindings;

  if (!this._iterator) {
    // Resolve all triple patterns in the graph
    this.forceResolveGraph(function (graph) {
      // Create an iterator that iterates over all bindings in the graph
      self._iterator = new GraphBindingsIterator(graph);

      // Read the bindings
      bindings = self._iterator.read();
      bindings && self._push(bindings);
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

module.exports = TripleGraphIterator;
