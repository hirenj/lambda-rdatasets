'use strict';

let runner = require('.');
let bucket_name = process.env['BUILD_BUCKET'];
let key = process.argv[2];
console.log(`Reading file from s3://${bucket_name}/${key} and writing to rdata/`)
runner.do_transform_s3(`s3://${bucket_name}/${key}`,'rdata/').then( () => console.log("OK"));