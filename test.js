'use strict';

let runner = require('./js/common');
console.log(process.argv[2]);
runner.do_transform('RData', process.argv[2].indexOf('//') < 0 ? ('file://'+process.argv[2]) : process.argv[2],'rdata/').catch( err =>
  console.log(err)
);
