'use strict';
/*jshint esversion: 6, node:true */

let config = {};

let bucket_name = 'data';

try {
  config = require('./resources.conf.json');
  bucket_name = config.buckets.dataBucket;
} catch (e) {
}

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
}
const AWS = require('lambda-helpers').AWS;
const s3 = new AWS.S3();

const extract_changed_keys = function(event) {
  if ( ! event.Records ) {
    return [];
  }
  let results = event.Records
  .filter( rec => rec.Sns )
  .map( rec => {
    let sns_message = JSON.parse(rec.Sns.Message);
    throw new Error("Need to parse message format");
  });
  results = [].concat.apply([],results);
  return results.filter( obj => obj.bucket == bucket_name ).map( obj => obj.key );
};

const serialiseDataset = function(event,context) {
  let changed_keys = extract_changed_keys(event);

  Promise.resolve().then( () => {
    context.succeed('OK');
  }).catch( err => {
    console.error(err);
    console.error(err.stack);
    context.fail('NOT-OK');
  });
};

exports.serialiseDataset = serialiseDataset;