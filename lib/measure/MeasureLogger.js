var fs = require('fs'),
    util = require('util');

function CSVLogger(file, header) {
  this.stream = fs.createWriteStream(file);
  this.header = header;
  header && this.writeHeader(header);
}

CSVLogger.prototype.writeHeader = function (header) {
  var quotedHeader = header.map(function (s) {
    return quoteString(s);
  });
  this.write(quotedHeader);
};

CSVLogger.prototype.write = function () {
  var line;
  if (arguments.length == 1)
    line = arguments[0];
  else
    line = Array.prototype.slice.call(arguments, 0);

  if (line instanceof Array)
    line = line.join(',');

  this.stream.write(line + '\n');
};

CSVLogger.prototype.log = function () {
  if (this.header && arguments.length != this.header.length)
    throw new Error('Header has length ' + this.header.length + ', but only ' + arguments.length + ' fields were given');

  this.write(Array.prototype.slice.call(arguments, 0));
};

CSVLogger.prototype.close = function () {
  this.stream.close();
};

function MeasureLogger(file, header, eventHandlers) {
  CSVLogger.call(this, file, header);
  this.eventHandlers = eventHandlers;
  this.row = [];
}
util.inherits(MeasureLogger, CSVLogger);

MeasureLogger.prototype.on = function (eventName, time, args) {
  var handler = this.eventHandlers[eventName];
  handler && handler.call(this, time, args);
};

MeasureLogger.prototype._isRowSet = function () {
  return !this.row.some(function (el) {
    return typeof(el) === 'undefined';
  }) && this.row.length == this.header.length;
};

MeasureLogger.prototype._flushRow = function () {
  this.write(this.row);
  this.row = [];
};

MeasureLogger.prototype.setColumn = function (index, value) {
  var rowValue = this.row[index];
  if (typeof rowValue !== 'undefined') throw new Error('Column ' + index + ' is already set to the value ' + rowValue);

  // Set the actual value
  this.row[index] = value;

  // Flush the row when all fields are set
  this._isRowSet() && this._flushRow();
};

MeasureLogger.prototype.setTriple = function (index, triple) {
  this.setString(index, tripleToString(triple));
};

MeasureLogger.prototype.setString = function (index, string) {
  this.setColumn(index, quoteString(string));
};

MeasureLogger.prototype.setNumber = function (index, number) {
  this.setColumn(index, number);
};

MeasureLogger.prototype.setBoolean = function (index, value) {
  this.setColumn(index, value ? 'true' : 'false');
};

// Helper functions
function tripleToString(triple) {
  return triple.subject + ' ' + triple.predicate + ' ' + triple.object;
}

function quoteString(string) {
  var res = '"';
  string && (res += string.replace(/["']/g, "\\$&"));
  res += '"';
  return res;
}

// Actual loggers
var TripleExecutionTimeLogger = new MeasureLogger('/tmp/triple_execution_times.csv', [ 'triple', 'number of bindings', 'request calculation time', 'request execution time', 'graph propagation time', 'is bound', 'bound variable'], {
  'execute request': function (requestExecutionTime, args) {
    var request = args.request,
        bindings = args.bindings;

    this.setTriple(0, request.triple);
    this.setNumber(1, bindings.length);
    this.setNumber(3, requestExecutionTime);
    this.setBoolean(5, request.isBound);
    this.setString(6, request.boundVariable);
  },
  'propagate triple bindings': function (triplePropagationTime, args) {
    this.setNumber(4, triplePropagationTime);
  },
  'calculate cheapest request': function (cheapestRequestCalculationTime, args) {
    this.setNumber(2, cheapestRequestCalculationTime);
  }
});

var LOGGERS = [
  TripleExecutionTimeLogger
];

function logMeasure(eventName, time, args) {
  LOGGERS.forEach(function (logger) {
    logger.on(eventName, time, args);
  });
}

module.exports = {
  CSVLogger: CSVLogger,
  TripleExecutionTimeLogger: TripleExecutionTimeLogger,
  logMeasure: logMeasure
};
