var _ = require('underscore');

var Iterator = require('../iterators/Iterator'),
    TriplePatternIterator = require('./TriplePatternIterator'),
    tripleGraph = require('triple-graph'),
    Graph = tripleGraph.Graph,
    CheapestRequestIterator = tripleGraph.CheapestRequestIterator,
    GraphBindingsIterator = tripleGraph.GraphBindingsIterator,
    Util = tripleGraph.Util,
    collectors = require('../iterators/collectors'),
    joins = require('lodash-joins'),
    innerJoin = joins.sortedMergeInnerJoin,
    leftJoin = joins.sortedMergeLeftOuterJoin;

var MeasureLogger = require('../measure/MeasureLogger');

var SUBJECT_COUNT_KEY = "subjectsPerObjectCount";
var OBJECT_COUNT_KEY = "objectsPerSubjectCount";

// Maps a bindingsList to a single object that maps each variable to an array of values that bind that variable
function mapBindings(bindingsList, variables) {
  var object = {};
  // Map each variable to undefined
  variables.forEach(function (variable) {
    object[variable] = undefined;
  });

  // Go over all bindings and push them at the end of each array
  bindingsList.forEach(function (bindings) {
    variables.forEach(function (variable) {
      var binding = bindings[variable];
      if (!binding) return;

      var array = object[variable] = object[variable] || [];
      array.push(binding);
    });
  });

  return object;
}

function TripleGraphIterator(parent, pattern, options) {
  // Empty patterns return no bindings
  if (!pattern || !pattern.length)
    return new Iterator.passthrough(parent, options);
  // A single triple can be solved quicker by a triple pattern iterator
  // if (pattern.length === 1)
  //   return new TriplePatternIterator(parent, pattern[0], options);
  Iterator.call(this, options);

  this._options = options;
  this._pattern = pattern;
  this._client = this._options.fragmentsClient;
  this._parent = parent;

  this.GraphLogger = MeasureLogger.logger.newGraphLogger(pattern);
  this.GraphLogger.startBuildingGraph();
  // Create a graph and set it up
  var graph = this._graph = new Graph();
  pattern.forEach(function (triplePattern) {
    graph.addTriple(triplePattern);
  });
  this.GraphLogger.stopBuildingGraph(graph);

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
      if (!parentGraph) return callback(true);

      // Add the bindings for all variables from the parent graph in the child graph
      self._graph.nodes.forEach(function (node) {
        if (node.isVariable) {
          var parentNode = parentGraph.findNodeByValue(node.variable);
          if (!parentNode) return;

          // The parent node should have bindings
          if (!parentNode.hasBindings)
            throw new Error("Parent graph doesn't have any binings for node '" + node.variable + "'");
          // Add these bindings to the corresponding node in the child graph
          node.addBindings(parentNode.bindings);
        }
      });

      // Set the parent bindings iterator
      self._parentBindingsIterator = parent;

      callback();
    });
  } // Otherwise we need to collect them by reading the iterator
  else {
    // Collect all bindings
    collectors.arrayCollector(parent, function (bindingsList) {
      if (!bindingsList) return callback();

      // Get all bindings for all variables in the current graph
      var nodes = self._graph.nodes.filter(function (node) { return node.isVariable; }),
          variables = nodes.map(function (node) { return node.variable; }),
          bindingsMap = mapBindings(bindingsList, variables);

      // Add all matching bindings to their respective nodes
      nodes.forEach(function (node) {
        var foundBindings = bindingsMap[node.variable];
        foundBindings && node.addBindings(foundBindings);
      });

      // Set the parent bindings
      self._parentBindings = bindingsList;

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

      // If triple count is zero or not found there will be no bindings
      if (!metadata.totalTriples) return callback(true);

      var foundMetadata = {
        pageSize: 100, // TODO: fetch pageSize from metadata
        tripleCount: metadata.totalTriples
      };

      // Extract count metadata
      if (metadata[SUBJECT_COUNT_KEY]) foundMetadata.subjectsPerObjectCount = metadata[SUBJECT_COUNT_KEY];
      if (metadata[OBJECT_COUNT_KEY]) foundMetadata.objectsPerSubjectCount = metadata[OBJECT_COUNT_KEY];

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
    self._executeDirectRequest(triplePattern, bindings, function (requestedBindings) {
      // Add the requested bindings to the bindings array
      requestedBindings && requestedBindings.length > 0 && allBindings.push(requestedBindings);
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
  this.GraphLogger.startRequestTriple();

  var graph = this._graph,
      self = this,
      bindingsCallback = function (bindings) {
        self.GraphLogger.stopRequestTriple();
        // Update the bindings in the graph
        self.GraphLogger.startPropagatingBindings();
        graph.updateTripleBindings(request.triple, bindings || []);
        self.GraphLogger.stopPropagatingBindings();
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

  this.GraphLogger.startCalculatingCheapestRequest();
  if (!it.hasNext()) {
    this.GraphLogger.stopCalculatingCheapestRequest();
    return callback();
  }

  // Empty bindings mean there are no results, so stop executing requests
  if (this._graph.hasEmptyBindings()) {
    this.GraphLogger.stopCalculatingCheapestRequest();
    return callback(true);
  }

  var next = it.next();
  this.GraphLogger.stopCalculatingCheapestRequest(next);

  this._executeRequest(next, function () {
    self._executeAllCheapRequests(callback);
  });
};

var UNRESOLVED = 0, RESOLVING = 1, RESOLVED = 2;
// Forces all triple patterns in the graph to be bound
TripleGraphIterator.prototype.forceResolveGraph = function (callback) {
  var callbacks = this._graphCallbacks = this._graphCallbacks || [];
  this._graphStatus = this._graphStatus || UNRESOLVED;

  if (this._graphStatus == RESOLVED) return callback(this._graph);
  if (this._graphStatus == RESOLVING) return callbacks.push(callback);

  this._graphStatus = RESOLVING;
  callbacks.push(callback);

  var self = this;

  // Returns the given graph through all callbacks
  function callCallbacks(graph) {
    self._graphStatus = RESOLVED;
    callbacks.forEach(function (callback) {
      callback(graph);
    });
  }

  // Fill the graph with bindings from the parent iterator first
  this.GraphLogger.startFillingGraph();
  this._fillGraph(this._parent, function (err) {
    self.GraphLogger.stopFillingGraph();
    // If something is wrong with the parent bindings, don't continue
    if (err) return callCallbacks(null);

    // Fetch the initial metadata
    self.GraphLogger.startFetchingMetadata();
    self._fetchMetadata(function (err) {
      self.GraphLogger.stopFetchingMetadata();

      if (err) return callCallbacks(null);

      // Loop over all requests and execute them in turn,
      // updating the graph after each request
      self._executeAllCheapRequests(function (err) {
        // Return the graph
        callCallbacks(err ? null : self._graph);
      });
    });
  });
};

TripleGraphIterator.prototype._mergeBindings = function (parentBindings, bindingsList) {
  var optional = this._options.optional;

  if (parentBindings.length === 0) return parentBindings;
  if (bindingsList.length === 0) return optional ? parentBindings : bindingsList;

  // Find the common variables between the two binding lists
  var commonVariables = _.intersection(Object.keys(parentBindings[0]),
      Object.keys(bindingsList[0]));

  function accessor(bindings) {
    return commonVariables.map(function (variable) {
      return bindings[variable];
    });
  }

  var join = optional ? leftJoin : innerJoin;
  return join(parentBindings, accessor, bindingsList, accessor);
};

TripleGraphIterator.prototype._mergeWithParentBindings = function (bindingsList, callback) {
  if (!this._parent) return callback(bindingsList);

  var self = this;

  // Get all the bindings from the parent
  if (this._parentBindings) {
    callback(this._mergeBindings(this._parentBindings, bindingsList));
  } else {
    collectors.arrayCollector(this._parentBindingsIterator, function (parentBindings) {
      callback(self._mergeBindings(parentBindings, bindingsList));
    });
  }
};

TripleGraphIterator.prototype._read = function () {
  var self = this,
      bindings;

  this._readStatus = this._readStatus || UNRESOLVED;

  if (!this._iterator) {
    // If we are in the middle of resolving the iterator, we should just wait it out
    if (this._readStatus == RESOLVING) return;

    this._readStatus = RESOLVING;
    // Resolve all triple patterns in the graph
    this.forceResolveGraph(function (graph) {
      self._readStatus = RESOLVED;
      if (!graph) return self._end();

      self.GraphLogger.startJoiningResults();
      var it = new GraphBindingsIterator(graph);

      // If we have a parent, we need to merge the bindings with the parent bindings
      if (self._parent) {
        self._mergeWithParentBindings(it.bindings, function (mergedBindings) {
          self._iterator = Iterator.fromArray(mergedBindings);
          readBindings();
        });
      } else {
        // Create an iterator that iterates over all bindings in the graph
        self._iterator = it;

        // Read the bindings from this iterator
        readBindings();
      }

      function readBindings() {
        // Read the bindings
        bindings = self._iterator.read();
        self.GraphLogger.stopJoiningResults();
        bindings ? self._push(bindings) : self._end();
      }
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
