var _ = require('underscore');

var Iterator = require('../iterators/Iterator'),
    TriplePatternIterator = require('./TriplePatternIterator'),
    tripleGraph = require('triple-graph'),
    Graph = tripleGraph.Graph,
    CheapestRequestIterator = tripleGraph.CheapestRequestIterator,
    GraphBindingsIterator = tripleGraph.GraphBindingsIterator,
    Util = tripleGraph.Util,
    collectors = require('../iterators/collectors'),
    TimeMeasure = require('../measure/TimeMeasure'),
    logMeasure = require('../measure/MeasureLogger').logMeasure;

var Measure = new TimeMeasure(logMeasure);

function TripleGraphIterator(pattern, options) {
  Iterator.call(this, options);

  this._options = options;
  this._pattern = pattern;
  this._client = this._options.fragmentsClient;

  // Create a graph and set it up
  Measure.start('build graph');
  var graph = this._graph = new Graph();
  pattern.forEach(function (triplePattern) {
    graph.addTriple(triplePattern);
  });
  Measure.stop('build graph');

  // Create a request iterator
  this._requestIterator = new CheapestRequestIterator(this._graph);
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
  Measure.start('execute request');
  var graph = this._graph,
      bindingsCallback = function (bindings) {
        Measure.stop('execute request', { request: request, bindings: bindings });

        // Update the bindings in the graph
        Measure.start('propagate triple bindings');
        graph.updateTripleBindings(request.triple, bindings);
        Measure.stop('propagate triple bindings');
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

  Measure.start('calculate cheapest request');
  if (!it.hasNext()) {
    Measure.clear('calculate cheapest request');
    return callback();
  }
  Measure.stop('calculate cheapest request');

  this._executeRequest(it.next(), function () {
    self._executeAllCheapRequests(callback);
  });
};

TripleGraphIterator.prototype._read = function () {
  var self = this,
      bindings;

  if (!this._iterator) {
    // Fetch the initial metadata first
    this._fetchMetadata(function () {
      // Loop over all requests and execute them in turn,
      // updating the graph after each request
      self._executeAllCheapRequests(function () {
        Measure.start('build bindings');
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
    if (bindings) {
      this._push(bindings);
    } else {
      Measure.stop('build bindings');
      this._end();
    }
  }
};

module.exports = TripleGraphIterator;
