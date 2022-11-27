const msdata_v1_4 = require('./msdata_1_4');
const msdata_v1_5 = require('./msdata_1_5');

const BY_VERSION = {
  "1.4": msdata_v1_4,
  "1.5": msdata_v1_5
};


const version = function(required) {
  return BY_VERSION[required] || latest();
}

const latest = function() {
  return BY_VERSION[Object.keys(BY_VERSION).sort()[0]];
}