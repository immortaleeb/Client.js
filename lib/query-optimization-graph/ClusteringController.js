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
  Stream = require('./Stream'),
  Cluster = require('./Cluster'),
  Node = require('./Node'),
  NeDBStoreInterface = require('./NeDBStoreInterface'),
  HashStoreInterface = require('./HashStoreInterface'),
  N3StoreInterface = require('./N3StoreInterface');

function ClusteringController (nodes, clusters) {
  this.clusters = clusters;
  this.nodes = nodes;
  this.logger = new Logger("ClusteringController");
  //this.logger.disable();
  //this.store = new RDFStoreInterface();
  //this.store = new NeDBStoreInterface();
  //this.store = new HashStoreInterface(nodes);
  this.store = new N3StoreInterface(nodes);
  this.results = {};
  this.DEBUGtime = 0;
  this.DEBUGstart = new Date();
  this.DEBUGtimer = {preread:0, read:0, http:0, read_pre:0, read_read:0, read_post:0, read_add_pre:0, read_add_read:0,
    read_add_post:0, postread:0, postread_updates:0, postread_prenode:0, postread_node:0, postread_feed:0, postread_stabilize:0};

  // TODO: make this real code
  var self = this;
  _.each(this.clusters, function (cluster) { cluster.DEBUGcontroller = self;});
}

ClusteringController.create = function (patterns, options, callback) {
  var clusters = {};
  var nodes = [];

  var delayedCallback = _.after(_.size(patterns), function () {
    var controller = new ClusteringController(nodes, clusters);
    setImmediate(callback(controller));
  });

  _.each(patterns, function (pattern) {
    var fragment = options.fragmentsClient.getFragmentByPattern(pattern);
    fragment.getProperty('metadata', function(metadata) {
      fragment.close();
      var node = new Node(pattern, metadata.totalTriples, options);
      nodes.push(node);
      var vars = ClusteringUtil.getVariables(pattern);
      _.each(vars, function (v) {
        clusters[v] = clusters[v] || new Cluster(v);
        clusters[v].nodes.push(node);
        node.DEBUGclusters[v] = clusters[v];
      });
      delayedCallback();
    });
  });
};

// TODO: don't allow nodes to supply to nodes that have a smaller count? (also not good if difference isn't big enough)
ClusteringController.prototype.start = function () {
  // start with best node to make sure supply gets called at least once
  var minNode = _.min(this.nodes, function (node) { return node.cost(); });

  // only minNode starts as download stream, rest becomes bound
  // TODO: won't work for unconnected parts
  var varsToUpdate = ClusteringUtil.getVariables(minNode.pattern);
  var varsDone = [];
  var parsedNodes = [minNode];
  while (!_.isEmpty(varsToUpdate)) {
    var v = varsToUpdate.shift();
    varsDone.push(v);
    var newVars = [];
    _.each(_.sortBy(this.clusters[v].nodes, function (node) { return node.fullStream.count; }), function (node) {
      // haven't updated this node yet
      if (node.activeStream === node.fullStream && node !== minNode && !node.fixed) {
        // TODO: this shouldn't be necessary, already create all binding streams?
        node.bindStreams[v] = new Stream.BindingStream(node.fullStream.count, node.pattern, v, node._options);
        node.activeStream = node.bindStreams[v];
        newVars = newVars.concat(ClusteringUtil.getVariables(node.pattern));
        parsedNodes.push(node);
      }
    });
    newVars = _.uniq(newVars);
    newVars = _.difference(newVars, varsDone);
    varsToUpdate = _.union(varsToUpdate, newVars);
  }

  var self = this;
  var changed = true;
  while (changed) {
    changed = false;
    _.each(this.nodes, function (node) {
      var bindVar = node.waitingFor();
      if (!bindVar) return;
      var suppliers = self.clusters[bindVar].suppliers();
      var full = _.every(suppliers, function (supplier) { return node.fullStream.count < supplier.fullStream.count/100; }); // TODO: pagesize
      changed |= full;
      if (full) {
        node.activeStream = node.fullStream;
      } else {
        var minVar = _.min(ClusteringUtil.getVariables(node.pattern), function (v) {
          var suppliers = _.without(self.clusters[v].suppliers(), node);
          return _.min(_.map(suppliers, function (supplier) { return supplier.fullStream.count; }));
        });
        var switchBindVar = minVar !== bindVar;
        changed |= switchBindVar;
        if (switchBindVar) {
          // TODO: this shouldn't be necessary, already create all binding streams?
          node.bindStreams[minVar] = new Stream.BindingStream(node.fullStream.count, node.pattern, minVar, node._options);
          node.activeStream = node.bindStreams[minVar];
        }
      }
    });
  }

  parsedNodes = _.sortBy(_.filter(this.nodes, function (node) { return node.activeStream === node.fullStream; }), function (node) {return node.fullStream.count;});
  var vars = _.union.apply(null, _.map(parsedNodes, function (node) { return ClusteringUtil.getVariables(node.pattern); }));
  while (parsedNodes.length < this.nodes.length) {
    var remaining = _.filter(this.nodes, function (node) { return !_.contains(parsedNodes, node) && _.intersection(vars, ClusteringUtil.getVariables(node.pattern)).length > 0;  });
    var best = _.min(remaining, function (node) { return node.fullStream.count; });
    parsedNodes.push(best);
    vars = _.union(vars, ClusteringUtil.getVariables(best.pattern));
  }

  //this.nodes = _.sortBy(this.nodes, function (node) { return node.fullStream.count; }); // TODO: breaks var fixing
  this.nodes = parsedNodes;

  _.each(this.nodes, function (node) {
    node.updateDependency();
    node.logger.info("initial bindVar: " + node.activeStream.bindVar + " (" + node.fullStream.count + ")");
  });

  this.logger.info("node order: " + _.map(_.pluck(this.nodes, 'pattern'), rdf.toQuickString));

  this.readNode(minNode);
};

ClusteringController.prototype.read = function () {
  //var minNode = _.min(this.nodes, function (node) { return node.cost(); });
//  var votes = _.object(_.map(this.clusters, function (cluster) { return [cluster.v, cluster.vote()]; }));
//  var hungryVotes = _.filter(votes, function (vote, v) { return vote && _.contains(hungryVars, v); });
//  var filteredVotes = _.isEmpty(hungryVotes) ? _.filter(votes) : hungryVotes;
//  var minNode = _.min(filteredVotes, function (node) { return node.cost(); });

//  var hungryVars = []; //_.uniq(_.filter(_.invoke(this.nodes, 'waitingFor')));
//  var hungryClusters = _.filter(this.clusters, function (cluster) { return _.contains(hungryVars, cluster.v); });
//  if (_.isEmpty(hungryClusters))
//    hungryClusters = this.clusters;
  var start = new Date();
  var votes = {};
  var self = this;
  var delayedCallback = _.after(_.size(this.clusters), function () {
    votes = _.omit(votes, _.isNull);
    // TODO: note to self: clusters will always be needed to store estimates per variable
    var boundNodes = _.filter(votes, function (node) { return node.activeStream.feed; });
    var suppliers = _.flatten(_.map(boundNodes, function (node) {
      if (node.activeStream.bindVar) {
        return _.without(self.clusters[node.activeStream.bindVar].supplyPath([node]), node);
      }
    }));
    // TODO: is it possible we end up with an empty list?
    var candidates = _.reject(votes, function (node) { return _.contains(suppliers, node); });
    var minNode = _.min(candidates, function (node) { return node.cost(); });

    var votingResults = _.map(votes, function (node, v) {
      if (!node)
        return null;

      var patternStr = v + ":" + rdf.toQuickString(node.pattern) + "(" + node.activeStream.bindVar + ")";

      if (node === minNode)
        patternStr = "*" + patternStr + "*";
      return patternStr;
    });
    //self.logger.info("requested: " + hungryVars);
    self.logger.info("votes: " + votingResults);

    self.DEBUGtimer.preread += new Date()-start;
    if (minNode === Infinity) {
      self.DEBUGtime = new Date() - self.DEBUGstart;
      console.log("RDF match time: " + Math.floor(self.store.DEBUGtime));
      console.log("HTTP call time: " +  Math.floor(self.nodes[0]._options.fragmentsClient._client.DEBUGtime));
      console.log("Remaining time: " + Math.floor(self.DEBUGtime - self.store.DEBUGtime - self.nodes[0]._options.fragmentsClient._client.DEBUGtime));
      console.log("Total time: " + self.DEBUGtime);
      console.log(_.map(_.keys(self.DEBUGtimer), function (key) { return key + ': ' + self.DEBUGtimer[key]; }).join(', '));
      return self.logger.info('Finished, totally not a bug!');
    } else {
      self.readNode(minNode);
    }
  });
  _.each(this.clusters, function (cluster) {
    cluster.vote(function (node) {
      votes[cluster.v] = node;
      delayedCallback();
    });
  });
};

// TODO: start a read while we are updating? (since we need to wait on http response anyway
ClusteringController.prototype.readNode = function (minNode) {
  var minCost = minNode.cost();
  var self = this;
  self.logger.info("cost: " + minCost);
  if (minCost > 0)
    _.each(self.nodes, function (node) { node.spend(minCost); });
  var start = new Date();
  minNode.read(function (add, remove) {
    self.DEBUGtimer.read += new Date()-start;
    self.DEBUGtimer.http = Math.floor(self.nodes[0]._options.fragmentsClient._client.DEBUGtime);
    var startPostread = new Date();
//    _.each(add, function (triple) {
//      _.each(ClusteringUtil.getVariables(minNode.pattern), function (v) {
//        if (!minNode.store[v][triple[minNode.getVariablePosition(v)]])
//          minNode.store[v][triple[minNode.getVariablePosition(v)]] = {};
//        minNode.store[v][triple[minNode.getVariablePosition(v)]][rdf.toQuickString(triple)] = triple;
//      });
//    });
//    self.store.addTriples(add, function () {
//      // TODO: timing
////      var vars = ClusteringUtil.getVariables(minNode.pattern);
////      _.each(vars, function (v) {
////        var pos = minNode.getVariablePosition(v);
////        self.clusters[v].removeBindings(_.filter(_.pluck(remove, pos)));
////        self.clusters[v].addBindings(_.filter(_.pluck(add, pos)));
////        if (minNode.ended()) {
////          var bounds = _.uniq(_.pluck(minNode.triples, pos));
////          self.clusters[v].addBounds(bounds);
////        }
////      });
//    });

    // TODO: switching to download stream here, maybe I can do this on a per cluster basis? (not sure if safe, should check)
    // TODO: maybe also move switching binding streams?

    start = new Date();
    var delayedCallback = _.after(_.size(self.clusters), function () {
      self.DEBUGtimer.postread_updates += new Date()-start;
      // only switch to download stream if we are sure it is for the best
      // TODO: estimate can be wonky at start if there are multiple streams, need better value
      // TODO: not sure of best time yet, need to be after supply to have estimates?
      // start with the cheapest node and continue until we find an acceptable switch (or no nodes are left)
      _.some(_.sortBy(_.filter(self.nodes, function (node) { return node.activeStream.bindVar; }), function (node) { return node.fullStream.count; }), function (node) {
        var v = node.activeStream.bindVar; // TODO: will this always be the correct choice?
        // value will be infinite if no values have been matched yet
        if (_.isFinite(self.clusters[v].estimate) && self.clusters[v].estimate > node.fullStream.cost) {
          node.logger.info("SWITCH STREAM " + v + " -> " + undefined + ", estimate: " + self.clusters[v].estimate + ", cost: " + node.fullStream.cost);
          node.activeStream = node.fullStream;
          _.each(self.nodes, function (node) {
            node.updateDependency();
          });
          // TODO: not sure if necessary
          // move node to the front next to the other download streams
//          var idx = _.indexOf(self.nodes, node);
//          self.nodes.splice(idx, 1);
//          var insertIdx = _.findIndex(self.nodes, function (node) { return !node.activeStream.bindVar; });
//          if (insertIdx < 0)
//            insertIdx = self.nodes.length;
//          self.nodes.splice(insertIdx, 0, node);
          return true; // found a match, wait until next iteration to try again
        }
      });

      // TODO: DEBUG let's see how many results we have
      self.store.matchBindings(_.pluck(self.nodes, 'pattern'), function (results) {
        self.logger.info("COMPLETE MATCHES: " + _.size(results));
        if (results.length < self.results.length)
          self.logger.info("RESULTS DECREASED!");
        var gotResults = _.size(results) > _.size(self.results);
        _.each(results, function (result) {
          // this is actually way too slow
//          if (!ClusteringUtil.containsObject(self.results, result)) {
//            self.results.push(result);
//            console.log(result);
//          }
          var str = bindingToString(result);
          if (!_.has(self.results, str)) {
            self.results[str] = result;
            console.log(result);
          }
        });

        function bindingToString (binding) {
          // TODO: keys can be cached
          var keys = _.keys(binding).sort();
          return _.map(keys, function (key) { return key+' '+binding[key]; }).join(' ');
        }
        self.DEBUGtimer.postread += new Date() - startPostread;

        if (gotResults) {
          self.DEBUGtime = new Date() - self.DEBUGstart;
          console.log("RDF match time: " + Math.floor(self.store.DEBUGtime));
          console.log("HTTP call time: " +  Math.floor(self.nodes[0]._options.fragmentsClient._client.DEBUGtime));
          console.log("Remaining time: " + Math.floor(self.DEBUGtime - self.store.DEBUGtime - self.nodes[0]._options.fragmentsClient._client.DEBUGtime));
          console.log("Total time: " + self.DEBUGtime);
          console.log(_.map(_.keys(self.DEBUGtimer), function (key) { return key + ': ' + self.DEBUGtimer[key]; }).join(', '));
        }

//        if (results.length >= 10) {
//          self.logger.info("TOTAL TIME: " + (new Date() - self.DEBUGstart));
//          process.exit(0);
//        }

        setImmediate(function () { self.read(); });
      });
    });
    _.each(self.clusters, function (cluster) {
      cluster.update(minNode, delayedCallback);
    });
  });
};

//ClusteringController.prototype.read2 = function () {
//  var minNode = _.min(this.nodes, function (node) { return node.cost(); });
//
//  if (minNode === Infinity)
//    return console.error('Finished, totally not a bug!');
//
//  var minCost = minNode.cost();
//
//  // TODO: count unique values on page download -> more is good (/total ?)
//  var self = this;
//  _.each(this.nodes, function (node) {
//    // TODO: problem: expensive streams, need to detect bound or not
//    node.read2(minCost, function (add, remove) {
//      // TODO: count each value?
//      var vars = ClusteringUtil.getVariables(node.pattern);
//      _.each(vars, function (v) {
//        self.clusters[v].removeBindings(_.filter(_.pluck(remove, v)));
//        if (node.supplies(v))
//          self.clusters[v].addBindings(_.filter(_.pluck(add, v)));
//        // TODO: bounds and stuff
//      });
//    });
//  });
//};

//ClusteringController.prototype.getAllPaths = function (node1, node2, varsUsed, used) {
//  if (node1 === node2 && !_.isEmpty(used))
//    return [[node1]];
//  used = used || [];
//  used = used.concat([node1.pattern]);
//  var self = this;
//  var legalNeighbours = _.flatten(_.map(_.difference(ClusteringUtil.getVariables(node1.pattern), varsUsed), function (v) { return self.clusters[v].nodes; }));
//  legalNeighbours = _.filter(legalNeighbours, function (node) { return !ClusteringUtil.containsObject(used, node.pattern); });
//  legalNeighbours = _.uniq(legalNeighbours, function (node) { return rdf.toQuickString(node.pattern); });
//  var paths = _.flatten(_.map(legalNeighbours, function (node) {
//    // TODO: possibly incorrect if multiple vars match or one var occurs multiple times
//    var v = _.first(_.difference(_.intersection(ClusteringUtil.getVariables(node1.pattern), ClusteringUtil.getVariables(node.pattern)), varsUsed));
//    var neighbourPaths = self.getAllPaths(node, node2, varsUsed.concat([v]), used);
//    return _.map(neighbourPaths, function (path) { return [node1].concat(path); });
//  }), true);
//  return paths;
//};

//ClusteringController.prototype.validatePath = function (binding, path) {
//  // TODO: starting from both sides probably faster
//  var validBindings = [binding];
//  while (!_.isEmpty(path) && !_.isEmpty(validBindings)) {
//    var node = _.first(path);
//    path = _.rest(path);
//    validBindings = _.flatten(_.map(validBindings, function (binding) {
//      // TODO: lots of double work prolly
//      return _.filter(_.map(node.activeStream.triples, function (triple) {
//        try { return rdf.extendBindings(binding, node.pattern, triple);}
//        catch (bindingError) { return null; }
//      }));
//    }));
//  }
//  return !_.isEmpty(validBindings);
//};

module.exports = ClusteringController;