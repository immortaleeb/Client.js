function TimeMeasure() {
  this.startTimes = {};
  this.deltas = {};
}

TimeMeasure.prototype.start = function (name) {
  if (this.startTimes[name])
    throw "There is a already a time running for " + name;
  this.startTimes[name] = new Date();
};

TimeMeasure.prototype.stop = function (name) {
  var delta = new Date() - this.startTimes[name];
  delete this.startTimes[name];
  return this.deltas[name] = delta;
};

TimeMeasure.prototype.get = function (name) {
  return this.deltas[name];
};

module.exports = TimeMeasure;
