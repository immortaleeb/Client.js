/**
 * Created by joachimvh on 11/09/2014.
 */

var rdf = require('../util/RdfUtil'),
  _ = require('lodash'),
  TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator'),
  Iterator = require('../iterators/Iterator'),
  MultiTransformIterator = require('../iterators/MultiTransformIterator'),
  Logger = require ('../util/Logger'),
  ClusteringUtil = require('./ClusteringUtil'),
  RDFStoreInterface = require('./RDFStoreInterface'),
  util = require('util');

function Stream (cost, pattern)
{
  this.cost = cost;
  this.costRemaining = cost;
  this.pattern = pattern; // we (usually) don't want multiple streams on the same pattern, this allows for identification
  this.vars = ClusteringUtil.getVariables(pattern);
  this.ended = false;
}

Stream.prototype.read = function (callback) {
  throw new Error('Not implemented yet.');
};

Stream.prototype.spend = function (cost) {
  this.costRemaining -= cost;
};

///////////////////////////// DownloadStream /////////////////////////////
function DownloadStream (pattern, count, options) {
  Stream.call(this, count/100, pattern); // TODO: pagesize

  this._iterator = new TriplePatternIterator(Iterator.single({}), pattern, options);
  this.remaining = count;
  this.count = count;
}
util.inherits(DownloadStream, Stream);

DownloadStream.prototype.read = function (callback) {
  if (this._iterator.ended)
    return setImmediate(function () { callback([]); });

  var self = this;
  var pageSize = 100; // TODO: real pagesize
  var buffer = [];
  var iterator = this._iterator;iterator.setMaxListeners(1000); // TODO: listeners
  iterator.on('data', addTriple);
  iterator.on('end', end);
  function addTriple (val) {
    //buffer.push(rdf.applyBindings(val, self.pattern));
    buffer.push(val);
    if (buffer.length >= pageSize || iterator.ended) {
      iterator.removeListener('data', addTriple);
      iterator.removeListener('end', end);
      self.remaining -= buffer.length;
      setImmediate(function (){ callback(buffer); });
    }
  }
  function end () {
    self.remaining = 0;
    self.ended = true;
    setImmediate(function (){ callback(buffer); });
  }

  this.cost = Math.max(0, this.remaining - pageSize)/pageSize;
  this.costRemaining = this.cost; // reset since we did a read
};

DownloadStream.prototype.close = function () {
  this._iterator.close();
};

///////////////////////////// BindingStream /////////////////////////////
function BindingStream (cost, pattern, bindVar, options) {
  Stream.call(this, cost, pattern);

  this.bindVar = bindVar;
  this._options = options;
  this._bindings = [];
  this.results = [];
  this._streams = [];
  this._gotAllData = false;
  this.ended = true; // unended when updating remaining
  this.remaining = Infinity;
  this.cost = Infinity;
  this.costRemaining = Infinity;
  this.count = Infinity;
}
util.inherits(BindingStream, Stream);

BindingStream.prototype.resultsPerBinding = function (results) {
  results = results || this.results;
  if (_.isEmpty(results))
    return Infinity;
  // TODO: this is not really correct since we don't take pages into account, already add 1 to take into account empty pages still being a call
  return ClusteringUtil.sum(_.map(results, function (result) { return Math.max(1, _.first(_.values(result))); } )) / _.size(results);
};

BindingStream.prototype.isStable = function () {
  if (this._gotAllData && _.isEmpty(this._bindings))
    return true;
  if (_.size(this.results) < 4)  // TODO: 4 is kind of random
    return false;
  var prev = _.initial(this.results);
  var prevAvg = this.resultsPerBinding(prev);
  var prevMargin = 1.96/(2*Math.sqrt(_.size(this.results)))*prevAvg;
  var avg = this.resultsPerBinding();
  return prevMargin*prevAvg > Math.abs(prevAvg-avg);
};

// TODO: it actually becomes harder to stabilize if we have more values ...
BindingStream.prototype.addBinding = function (callback) {
  var self = this;
  // we use a random index to try and reduce the effect of sorting on the data (if any)
  // TODO: not using random to debug more easily
  var idx = 0; //_.random(_.size(this._bindings)-1);
  var bindingVal = _.first(this._bindings.splice(idx, 1));
  var binding = _.object([[this.bindVar, bindingVal]]);
  var boundPattern = rdf.applyBindings(binding, this.pattern);
  var fragment = this._options.fragmentsClient.getFragmentByPattern(boundPattern);
  fragment.getProperty('metadata', function(metadata) {
    fragment.close();
    var stream = new DownloadStream(boundPattern, metadata.totalTriples, self._options);
    stream.bindVal = binding;
    self._streams.push(stream);
    self.results.push(_.object([[bindingVal, metadata.totalTriples]]));
    // advantage: quickly have a stable stream
    // disadvantage: only using data on the first page
    setImmediate(callback);
  });
};

BindingStream.prototype.stabilize = function (callback) {
  if (this.isStable())
    return callback(true);
  if (_.isEmpty(this._bindings))
    return callback(false);

  var self = this;
  this.addBinding(function () { self.stabilize(callback); });
};

BindingStream.prototype.read = function (callback, _recursive) {
  if (this.ended)
    return setImmediate(function () { callback([]); });

  var self = this;
  // always add at least 1 new binding if possible to update the stability
  if ((!_recursive || !this.isStable()) && !_.isEmpty(this._bindings)) {
    this.addBinding(function () { self.read(callback, true); });
  } else {
    var stream = _.first(this._streams);
    stream.read(function (buffer) {
      if (stream.ended)
        self._streams.shift();
      var remove = [];
      // there were no matches for this binding so it should be removed
      if (stream.count === 0)
        remove.push(_.object([[self.bindVar, stream.bindVal]]));

      self.cost -= _.size(buffer);
      self.costRemaining = self.cost;
      if (self.remaining <= 0 && _.isEmpty(self._streams) && _.isEmpty(self._bindings))
        self.ended = true;

      setImmediate(function () { callback(buffer, remove, stream.bindVal); });
    });
  }
};

BindingStream.prototype.feed = function (add, remove) {
  add = add || [];
  remove = remove || [];
  var resultBindings = _.keys(this.results);
  // don't add elements we already added before
  add = _.difference(add, resultBindings);
  this._bindings = _.union(this._bindings, add);
  var groupedResults = _.groupBy(this.results, function (count, binding) {
    return _.contains(remove, binding) ? "remove" : "keep";
  });
  groupedResults.remove = groupedResults.remove || [];
  groupedResults.keep = groupedResults.keep || [];
  this.results = groupedResults.keep;
  // remove unneeded streams
  this._streams = _.filter(this._streams, function (stream) {
    var remove = _.contains(remove, stream.bindVal);
    if (remove) stream.close();
    return !remove;
  });
};

BindingStream.prototype.updateRemaining = function (remaining) {
  this.ended = _.isEmpty(this._bindings) && _.every(this._streams, 'ended') && remaining === 0;
  this._gotAllData = remaining <= 0;

  if (!this.isStable()) {
    this.remaining = Infinity;
    this.cost = Infinity;
    this.costRemaining = Infinity;
    this.count = Infinity;
    return;
  }

  this.remaining = ClusteringUtil.sum(this._streams, 'remaining');
  this.remaining += (remaining + _.size(this._bindings)) * this.resultsPerBinding();

  var diff = this.cost < Infinity ? this.remaining - this.cost : 0;
  this.cost = this.remaining;
  this.costRemaining = Math.min(this.cost, this.costRemaining + diff);

  this.count = ClusteringUtil.sum(_.map(this.results, function (result) { return _.first(_.values(result)); }));
  this.count += _.size(this._bindings) * this.resultsPerBinding(); // _streams are already included in results
};


module.exports = Stream;
Stream.DownloadStream = DownloadStream;
Stream.BindingStream = BindingStream;