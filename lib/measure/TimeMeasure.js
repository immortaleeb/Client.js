function defaultOnMeasure(eventName, time, args) {
  // if (args) console.error('[TIME]', eventName, args, ':', (time / 1e6), 'ms');
  // else      console.error('[TIME]', eventName, ':', (time / 1e6), 'ms');
  console.error('[TIME]', eventName, ':', (time / 1e6), 'ms');
}

function TimeMeasure(onMeasure) {
  this.onMeasure = onMeasure || defaultOnMeasure;
  this._events = {};
}

TimeMeasure.prototype.start = function (eventName) {
  this._events[eventName] = process.hrtime();
};

TimeMeasure.prototype.startOnce = function (eventName, args) {
  !this._events[eventName] && this.start(eventName, args);
};

TimeMeasure.prototype._stop = function (eventName, last, args) {
  var diff = process.hrtime(last),
      time = diff[0] * 1e9 + diff[1];

  this.onMeasure(eventName, time, args);
  this.clear(eventName);
};

TimeMeasure.prototype.stop = function (eventName, args) {
  var last = this._events[eventName];
  if (!last) throw new Error('Unknown event ' + eventName);
  this._stop(eventName, last, args);
};

TimeMeasure.prototype.clear = function (eventName) {
  delete this._events[eventName];
};

TimeMeasure.prototype.mark = function (eventName) {
  var last = this._events[eventName];

  // Report last one
  last && this._stop(eventName, last);
  // Start a new one
  this.start(eventName);
};

var measure;
TimeMeasure.instance = function () {
  if (!measure)
    measure = new TimeMeasure();
  return measure;
};

module.exports = TimeMeasure;
