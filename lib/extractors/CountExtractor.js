/*! @license ©2014 Ruben Verborgh - Multimedia Lab / iMinds / Ghent University */
/* A CountExtractor extracts count metadata from a triple stream. */

var MetadataExtractor = require('./MetadataExtractor'),
    rdf = require('../util/RdfUtil');

var DEFAULT_COUNT_PREDICATES = toHash([rdf.VOID_TRIPLES, rdf.HYDRA_TOTALITEMS]);
var SO_COUNTS_PREFIX = 'http://example.org/stats#';
var SPO_COUNT_PREDICATE = SO_COUNTS_PREFIX + 'subjectsPerObjectCount';
var OPS_COUNT_PREDICATE = SO_COUNTS_PREFIX + 'objectsPerSubjectCount';

/**
 * Creates a new `CountExtractor`.
 * @classdesc A `CountExtractor` extracts count metadata from a triple stream.
 * @param {object} options
 * @param {string[]} [options.request=void:triples and hydra:totalItems] The count predicates to look for.
 * @constructor
 * @augments MetadataExtractor
 */
function CountExtractor(options) {
  if (!(this instanceof CountExtractor))
    return new CountExtractor(options);
  MetadataExtractor.call(this);
  this._countPredicates = toHash(options && options.countPredicates) || DEFAULT_COUNT_PREDICATES;
}
MetadataExtractor.inherits(CountExtractor);

/* Extracts metadata from the stream of triples. */
CountExtractor.prototype._extract = function (metadata, tripleStream, callback) {
  var countPredicates = this._countPredicates,
      foundMetadata = {};

  tripleStream.on('end', sendMetadata);
  tripleStream.on('data', extractCount);

  // Tries to extract count information from the triple
  function extractCount(triple) {
    // Total count
    if (triple.predicate in countPredicates &&
        rdf.decodedURIEquals(triple.subject, metadata.fragmentUrl) &&
        !foundMetadata.totalTriples) {
      var count = triple.object.match(/\d+/);
      count && (foundMetadata.totalTriples = parseInt(count[0], 10));
    } // subjects per object count
    else if (triple.predicate == SPO_COUNT_PREDICATE) {
      var spoCount = literalToFloat(triple.object);
      (spoCount !== null) && (foundMetadata.subjectsPerObjectCount = spoCount);
    } // objects per subject count
    else if (triple.predicate == OPS_COUNT_PREDICATE) {
      var opsCount = literalToFloat(triple.object);
      (opsCount !== null) && (foundMetadata.objectsPerSubjectCount = opsCount);
    }
  }
  // Sends the metadata through the callback and disables further extraction
  function sendMetadata(metadata) {
    tripleStream.removeListener('end', sendMetadata);
    tripleStream.removeListener('data', extractCount);
    callback(null, metadata || foundMetadata);
  }
};

// Converts a literatl string to a float value
function literalToFloat(literal) {
  var match = literal.match(/\d*\.?\d+/);
  if (!literal.match(/^"[0-9\.]+"/) || !match) {
    //throw new Error("Can not parse literal " + literal + " as float!");
    console.error('Warning: can not parse literal ' + literal + ' as float');
    return null;
  }
  return parseFloat(match[0]);
}

/**
 * Converts an array into an object with its values as keys.
 * @param {Array} array The keys for the object
 * @returns {Object} An object with the array's values as keys
 * @private
 */
function toHash(array) {
  return array && array.reduce(function (hash, key) { return hash[key] = true, hash; }, Object.create(null));
}

module.exports = CountExtractor;
