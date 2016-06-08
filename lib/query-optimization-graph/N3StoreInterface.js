/**
 * Created by joachimvh on 3/10/2014.
 */

var rdf = require('../util/RdfUtil'),
    _ = require('lodash'),
    N3 = require('N3'),
    Logger = require ('../util/Logger'),
    ClusteringUtil = require('./ClusteringUtil'),
    Iterator = require('../iterators/Iterator'),
    ReorderingGraphPatternIterator = require('../triple-pattern-fragments/ReorderingGraphPatternIterator'),
    TriplePatternIterator = require('../triple-pattern-fragments/TriplePatternIterator');

function N3StoreInterface (nodes) {
  this.store = N3.Store();
  this.nodes = nodes;
  this.nodeMap = _.object(_.map(nodes, function (node) { return rdf.toQuickString(node.pattern); }), nodes);
  this.cache = [];
  this.lastCounts = {};
  this.DEBUGtime = 0;
  this.logger = new Logger("N3StoreInterface");
  this.logger.disable();
}

N3StoreInterface.prototype.addTriples = function (triples, callback) {
  this.store.addTriples(triples);
  callback();
};

N3StoreInterface.prototype.cacheResult = function (nodes) {
  var strings = _.map(nodes, function (node) { return rdf.toQuickString(node.pattern); }).sort();
  var idx = _.findIndex(this.cache, function (cacheEntry) {
    if (strings.length !== cacheEntry.strings.length)
      return false;
    return _.intersection(cacheEntry.strings, strings).length === strings.length;
  });
  return idx < 0 ? null : this.cache[idx];
};

N3StoreInterface.prototype.cacheMatchesNodes = function (entry, nodes) {
  // assumes entry and nodes match up
  nodes = _.sortBy(nodes, function (node) { return rdf.toQuickString(node.pattern); });
  for (var i = 0; i < entry.strings.length; ++i) {
    if (entry.counts[entry.strings[i]] !== nodes[i].activeStream.tripleCount)
      return false;
  }
  return true;
};

N3StoreInterface.prototype.updateCacheResult = function (entry, nodes, bindings) {
  nodes = _.sortBy(nodes, function (node) { return rdf.toQuickString(node.pattern); });
  entry.counts = _.object(entry.strings, _.map(nodes, function (node) { return node.activeStream.tripleCount; }));
  entry.bindings = bindings;
};

N3StoreInterface.prototype.pushCacheResult = function (nodes, bindings) {
  var strings = _.map(nodes, function (node) { return rdf.toQuickString(node.pattern); });
  var counts = _.object(strings, _.map(nodes, function (node) { return node.activeStream.tripleCount; }));
  var entry = {strings:strings, counts:counts, bindings:bindings};
  this.cache.push(entry);
};

// TODO: giving patterns to keep interface, should accept nodes if used eventually
N3StoreInterface.prototype.matchBindings = function (patterns, callback) {
  var DEBUGdate = new Date();
  var self = this;
//  var it = new ReorderingGraphPatternIterator(Iterator.single({}), patterns, {fragmentsClient: new N3FragmentsClientWrapper(this.store)});

//  var parameters = _.map(['subject', 'predicate', 'object'], function (pos) { return rdf.isVariable(patterns[0][pos]) ? null : patterns[0][pos]; });
//  var data = this.store.find.apply(this.store, parameters);
//  var it = Iterator.ArrayIterator(data);

//  var count = 0;
//  it.toArray(function (error, data) {
//    self.DEBUGtime += new Date() - DEBUGdate;
//    callback(data);
//  });
  this.logger.info('query: ' + _.map(patterns, rdf.toQuickString).join(" "));

  var nodes = _.map(patterns, function (pattern) { return self.nodeMap[rdf.toQuickString(pattern)]; });
  nodes = _.sortBy(nodes, function (node) { return node.activeStream.tripleCount; });
  _.each(nodes, function (node) { if (!_.has(self.lastCounts, rdf.toQuickString(node.pattern))) self.lastCounts[rdf.toQuickString(node.pattern)] = 0; });
  var grouped = _.groupBy(nodes, function (node) {
    return node.activeStream.tripleCount === self.lastCounts[rdf.toQuickString(node.pattern)] ? 'same' : 'changed';
  });
  _.each(nodes, function (node) { self.lastCounts[rdf.toQuickString(node.pattern)] = node.activeStream.tripleCount; });
  grouped.same = grouped.same || [];
  grouped.changed = grouped.changed || [];
  nodes = grouped.same.concat(grouped.changed);
  var orderedNodes = [nodes.shift()];
  var vars = ClusteringUtil.getVariables(orderedNodes[0].pattern);
  while (!_.isEmpty(nodes)) {
    // TODO: unconnected nodes problem
    var nodeIdx = _.findIndex(nodes, function (node) { return !_.isEmpty(_.intersection(vars, ClusteringUtil.getVariables(node.pattern))); });
    var nextNode = nodes.splice(nodeIdx, 1)[0];
    orderedNodes.push(nextNode);
    vars = _.union(vars, ClusteringUtil.getVariables(nextNode.pattern));
  }

  _.each(_.keys(DEBUGTIMER), function (key) { DEBUGTIMER[key] = 0; });

  var start;
  var bindings = [Object.create(null)];
  var newBindings = [];
  for (var i = 0; i < orderedNodes.length; ++i) {
    if (_.isEmpty(bindings))
      break;
    var node = orderedNodes[i];
    var cacheEntry = this.cacheResult(orderedNodes.slice(0, i+1));
    if (cacheEntry && this.cacheMatchesNodes(cacheEntry, orderedNodes.slice(0, i+1))) {
      bindings = cacheEntry.bindings;
      self.logger.info('CACHED ' + i + ' ' + cacheEntry.strings);
    } else {
      // TODO: expect only 1 triple to trigger newTripleBindings?
      if (cacheEntry) {
        start = new Date();
        var newTripleBindings = [];
        if (cacheEntry.counts[rdf.toQuickString(node.pattern)] !== node.activeStream.tripleCount)
          newTripleBindings = this.extendBindings(bindings, node.pattern, node.newTriples);
        DEBUGTIMER.newTriples += new Date() - start;
        start = new Date();
        var updatedNewBindings = this.extendBindings(newBindings, node.pattern, this.patternFromStore(node.pattern));
        DEBUGTIMER.updatedTriples += new Date() - start;
        start = new Date();
        newBindings = newTripleBindings.concat(updatedNewBindings);
        bindings = newBindings.concat(cacheEntry.bindings);
      } else {
        start = new Date();
        bindings = this.extendBindings(bindings, node.pattern);
        DEBUGTIMER.basic += new Date() - start;
        start = new Date();
      }
      // TODO: way to make sure this wouldn't be necessary
      var uniq = _.uniq(bindings, function (binding) { return _.map(_.sortBy(_.keys(binding)), function (key) { return key+''+binding[key]; }).join(''); } );
      bindings = uniq;
      //node.newTriples = [];
      if (cacheEntry)
        this.updateCacheResult(cacheEntry, orderedNodes.slice(0, i+1), bindings);
      else
        this.pushCacheResult(orderedNodes.slice(0, i+1), bindings);
      DEBUGTIMER.rest += new Date() - start;
    }
  }
  this.DEBUGtime += new Date() - DEBUGdate;
//  this.logger.info("query time: " + (new Date() - DEBUGdate));
//  DEBUGTIMER.apply = Math.ceil(DEBUGTIMER.apply/1000000);
//  DEBUGTIMER.match = Math.ceil(DEBUGTIMER.match/1000000);
//  DEBUGTIMER.bind = Math.ceil(DEBUGTIMER.bind/1000000);
//  DEBUGTIMER.matchFilter = Math.ceil(DEBUGTIMER.matchFilter/1000000);
//  DEBUGTIMER.matchStore = Math.ceil(DEBUGTIMER.matchStore/1000000);
//  this.logger.info(DEBUGTIMER);
  callback(bindings);
};
var DEBUGTIMER = {newTriples:0, updatedTriples:0, basic:0, rest:0, apply:0, match:0, bind:0, matchFilter:0, matchStore:0};

N3StoreInterface.prototype.extendBindings = function (bindings, pattern, triples) {
  var self = this;
  return _.flatten(_.map(bindings, function (binding) {
    var start = process.hrtime();
    var appliedBinding = rdf.applyBindings(binding, pattern);
    DEBUGTIMER.apply += process.hrtime(start)[1];
    start = process.hrtime();
    var matches = triples ? self.filterPatternTriples(triples, appliedBinding) : self.patternFromStore(appliedBinding);
    DEBUGTIMER.match += process.hrtime(start)[1];
    start = process.hrtime();
    var result = self.triplesToBindings(_.clone(binding), appliedBinding, matches);
    DEBUGTIMER.bind += process.hrtime(start)[1];
    return result;
  }));
};

N3StoreInterface.prototype.filterPatternTriples = function (triples, pattern) {
  var start = process.hrtime();
  var result = _.filter(triples, function (triple) {
    return (rdf.isVariable(pattern.subject) || pattern.subject === triple.subject) &&
      (rdf.isVariable(pattern.object) || pattern.object === triple.object) &&
      (rdf.isVariable(pattern.predicate) || pattern.predicate === triple.predicate);
  });
  DEBUGTIMER.matchFilter += process.hrtime(start)[1];
  return result;
};

N3StoreInterface.prototype.patternFromStore = function (pattern) {
  var start = process.hrtime();
  var matches = this.store.find(
    rdf.isVariable(pattern.subject) ? null : pattern.subject,
    rdf.isVariable(pattern.predicate) ? null : pattern.predicate,
    rdf.isVariable(pattern.object) ? null : pattern.object
  );
  DEBUGTIMER.matchStore += process.hrtime(start)[1];
  return matches;
};

N3StoreInterface.prototype.triplesToBindings = function (binding, pattern, verifiedTriples) {
  var newBindings = _.map(verifiedTriples, function (triple) {
    var newBinding = _.clone(binding);
    if (rdf.isVariable(pattern.subject)) newBinding[pattern.subject] = triple.subject;
    if (rdf.isVariable(pattern.predicate)) newBinding[pattern.predicate] = triple.predicate;
    if (rdf.isVariable(pattern.object)) newBinding[pattern.object] = triple.object;
    return newBinding;
  });
  return newBindings;
};

function N3FragmentsClientWrapper (store) {
  this.store = store;
}

N3FragmentsClientWrapper.prototype.getFragmentByPattern = function (pattern) {
  var parameters = _.map(['subject', 'predicate', 'object'], function (pos) { return rdf.isVariable(pattern[pos]) ? null : pattern[pos]; });
  var data = this.store.find.apply(this.store, parameters);
  var iterator = Iterator.ArrayIterator(data);
  iterator.setProperty('metadata', {totalTriples: _.size(data)});
  return iterator;
};

module.exports = N3StoreInterface;