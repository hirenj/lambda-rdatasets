'use strict';

let runner = require('.');
console.log(process.argv[2]);
runner.do_transform(runner.transformers[0], process.argv[2].indexOf('//') < 0 ? ('file://'+process.argv[2]) : process.argv[2],'rdata/');