function TimeMeasure() {
  this.startTimes = {};
}

TimeMeasure.prototype.start = function (name) {
  if (this.startTimes[name])
    throw "There is a already a time running for " + name;
  this.startTimes[name] = Date.now();
};

TimeMeasure.prototype.stop = function (name) {
  var endTime = Date.now();
  var startTime = this.startTimes[name];
  if (!startTime) return undefined;

  var delta = endTime - startTime;

  var result = {
    start: startTime,
    end: endTime,
    delta: delta
  };

  delete this.startTimes[name];
  return result;
};

module.exports = TimeMeasure;
