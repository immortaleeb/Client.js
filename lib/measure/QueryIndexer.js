var fs = require('fs'),
    path = require('path');

// Gives a unique id to each query that is added and saves them to an index file
function QueryIndex(logDir) {
  this.nextIndex = 0;

  var indexFile = path.join(logDir, 'queryIndex.json');
  this.stream = fs.createWriteStream(indexFile);
  this.stream.write("{\n");
}

QueryIndex.prototype.add = function (query) {
  var queryIndex = this.nextIndex;

  if (!this._isFirstQuery()) {
    this.stream.write(",\n");
  }
  this.stream.write('  ' + queryIndex + ': ' + JSON.stringify(query.split("\n"), null, 2));

  this.nextIndex++;

  return queryIndex;
};

QueryIndex.prototype._isFirstQuery = function () {
  return this.nextIndex === 0;
};

QueryIndex.prototype.close = function () {
  this.stream.end("\n}");
};

module.exports = QueryIndex;
