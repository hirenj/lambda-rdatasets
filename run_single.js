'use strict';

let runner = require('.');
let bucket_name = process.env['BUILD_BUCKET'];
let key = process.argv[2];
let output_key = key.split('/')[1];
let serialiser = process.argv[3];
console.log(`Reading file from s3://${bucket_name}/${key} and writing to rdata/`)
runner.do_transform_s3(serialiser,`s3://${bucket_name}/${key}`,'rdata/')
      .then( (filedata) => runner.update_metadata(output_key,`${filedata.title}_${filedata.version}`) )
      .then( () => console.log("OK"));