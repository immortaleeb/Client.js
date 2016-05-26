var os = require('os');

var DEFAULT_MONITOR_UPDATE_RATE = 1000;
var DEFAULT_NAME = "\0";
var EMPTY_RESULT = { cpu: {}, mem: {} };

// Helper methods
function measureCpuTimes() {
  var cpus = os.cpus();
  var busy = 0;
  var idle = 0;

  for (var i = 0; i < cpus.length; i++) {
    var times = cpus[i].times;
    busy += times.user + times.sys;
    idle += times.idle;
  }

  return { busy: busy, idle: idle };
}

function calculateCpuUsage(times1, times2) {
  var busyDelta = times2.busy - times1.busy;
  var idleDelta = times2.idle - times1.idle;
  var totalDelta = busyDelta + idleDelta;
  return busyDelta / totalDelta;
}

function calculateAverage(arr) {
  var total = 0;
  for (var i = 0; i < arr.length; i++) {
    total += arr[i];
  }
  return total / arr.length;
}

// Monitor
function Monitor(interval) {
  this._isRunning = {};
  this.measurements = {};
  this.interval = interval || DEFAULT_MONITOR_UPDATE_RATE;
}

Monitor.prototype.isRunning = function (name) {
  name = name || DEFAULT_NAME;
  return !!this._isRunning[name];
};

Monitor.prototype.start = function (name) {
  name = name || DEFAULT_NAME;
  if (this.isRunning(name))
    throw new Error("The monitor can not be started for " + name + " because it is already running!");

  this._isRunning[name] = true;

  this._doMeasurement(name);
};

Monitor.prototype._doMeasurement = function (name) {
  if (!this.isRunning(name)) return;

  var measurement = measureCpuTimes();
  measurement.usedmem = os.totalmem() - os.freemem();

  var measurements = this.measurements[name] = this.measurements[name] || [];
  measurements.push(measurement);

  setTimeout(this._doMeasurement.bind(this), this.interval, name);
};

Monitor.prototype.stop = function (name) {
  name = name || DEFAULT_NAME;
  this._isRunning[name] = false;
  var results = this._produceResults(name);
  this._resetMeasurements(name);
  return results;
};

Monitor.prototype._produceResults = function (name) {
  var measurements = this.measurements[name];
  var cpuUsageList = [];
  if (measurements.length < 2) return EMPTY_RESULT;

  for (var i = 1; i < measurements.length; i++) {
    var firstMeasurement = measurements[i - 1];
    var secondMeasurement = measurements[i];

    var cpuUsage = calculateCpuUsage(firstMeasurement, secondMeasurement);
    cpuUsageList.push(cpuUsage);
  }

  var result = {};

  // Cpu results
  result.cpu = {
    all: cpuUsageList,
    min: cpuUsageList.reduce(function (prev, m) { return Math.min(prev, m); }, Number.MAX_VALUE),
    max: cpuUsageList.reduce(function (prev, m) { return Math.max(prev, m); }, -1),
    avg: calculateAverage(cpuUsageList)
  };

  // Mem results
  var usedmem = measurements.map(function (m) { return m.usedmem; });
  result.mem = {
    all: usedmem,
    min: usedmem.reduce(function (prev, m) { return Math.min(prev, m); }, Number.MAX_VALUE),
    max: usedmem.reduce(function (prev, m) { return Math.max(prev, m); }, -1),
    avg: calculateAverage(usedmem),
    total: os.totalmem()
  };

  return result;
};

Monitor.prototype._resetMeasurements = function (name) {
  this.measurements[name] = [];
};

module.exports = Monitor;
