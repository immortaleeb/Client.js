/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
var N3 = require('n3'),
    util = module.exports = N3.Util({});


/* Methods for variables and triple patterns */

// Indicates whether the entity represents a variable
util.isVariable = function (entity) {
  return !entity || /^urn:var#/.test(entity);
};

// Indicates whether the entity represents a variable or blank node
util.isVariableOrBlank = function (entity) {
  return !entity || /^urn:var#|^_:/.test(entity);
};

// Creates a filter for triples that match the given pattern
util.tripleFilter = function (triplePattern) {
  var pattern = triplePattern || {},
      subject   = util.isVariableOrBlank(pattern.subject)   ? null : pattern.subject,
      predicate = util.isVariableOrBlank(pattern.predicate) ? null : pattern.predicate,
      object    = util.isVariableOrBlank(pattern.object)    ? null : pattern.object;
  return function (triple) {
    return (subject === null   || subject   === triple.subject) &&
           (predicate === null || predicate === triple.predicate) &&
           (object === null    || object    === triple.object);
  };
};

// Find the bindings that transform the pattern into the triple
util.findBindings = function (triplePattern, boundTriple) {
  return util.addBindings(Object.create(null), triplePattern, boundTriple);
};

// Extend the bindings with those that transform the pattern into the triple
util.addBindings = function (bindings, triplePattern, boundTriple) {
  util.addBinding(bindings, triplePattern.subject,   boundTriple.subject);
  util.addBinding(bindings, triplePattern.predicate, boundTriple.predicate);
  util.addBinding(bindings, triplePattern.object,    boundTriple.object);
  return bindings;
};

// Extend the bindings with a binding that binds the left component to the right
util.addBinding = function (bindings, left, right) {
  // The left side maybe a variable; the right side may not
  if (util.isVariable(right))
    throw new Error('Right-hand side may not be a variable.');
  // If the left one is the variable
  if (util.isVariable(left)) {
    // Add it to the bindings if it wasn't already bound
    if (!(left in bindings))
      bindings[left] = right;
    // The right-hand side should be consistent with the binding
    else if (right !== bindings[left])
      throw new Error(['Cannot bind', left, 'to', right,
                       'because it was already bound to', bindings[left]].join(' '));
  }
  // Both are constants, so they should be equal for a successful binding
  else if (left !== right) {
    throw new Error(['Cannot bind', left, 'to', right].join(' '));
  }
  // Return the extended bindings
  return bindings;
};


/* Common RDF namespaces and URIs */

namespace('rdf', 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', [
  'type', 'subject', 'predicate', 'object',
]);

namespace('var', 'urn:var#');

namespace('dbpedia', 'http://dbpedia.org/resource/');
namespace('dbpedia-owl', 'http://dbpedia.org/ontology/');

function namespace(prefix, base, names) {
  var key = prefix.replace(/[^a-z]/g, '').toUpperCase();
  util[key] = base;
  names && names.forEach(function (name) {
    util[key + '_' + name.toUpperCase()] = base + name;
  });
}

Object.freeze(util);