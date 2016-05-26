/**
 * This module contains "collectors".
 * A collector is a function that takes an iterator as source and collects
 * all items over which the iterator iterates into a single "thing" (e.g: an array).
 */

function _arrayCollector(sourceIterator, items, callback) {
  var item;

  // Try to read the next item
  item = sourceIterator.read();

  // Does the iterator already have a next item?
  if (item === null) {
    // No items available yet, wait for the iterator to become readable
    function waitForReadable() {
      sourceIterator.removeListener('readable', waitForReadable);
      sourceIterator.removeListener('end', waitForEnd);
      // Recursively read the next item
      _arrayCollector(sourceIterator, items, callback);
    }
    sourceIterator.on('readable', waitForReadable);

    // The iterator might also end in the meanwhile
    function waitForEnd() {
      // On close, remove all listeners
      sourceIterator.removeListener('readable', waitForReadable);
      sourceIterator.removeListener('end', waitForEnd);

      // Return the results
      callback(items);
    }
    sourceIterator.on('end', waitForEnd);

  } // otherwise we've got a valid item
  else {
    // Add this item to the accumulator
    items = items || [];
    items.push(item);

    // If the iterator ended, we return our results, otherwise we recursively
    // fetch the nex item
    if (sourceIterator.ended)
      callback(items);
    else
      _arrayCollector(sourceIterator, items, callback);
  }
}

module.exports = {
  arrayCollector: function (sourceIterator, callback) {
    _arrayCollector(sourceIterator, undefined, callback);
  }
};