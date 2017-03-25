'use strict';
/*jshint esversion: 6, node:true */

const PassThrough = require('stream').PassThrough;

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
// const AWS = require('lambda-helpers').AWS;
// const s3 = new AWS.S3();
const RData = require('node-rdata');
const JSONStream = require('JSONStream');
const fs = require('fs');
const ConvertJSON = require('./js/transform').ConvertJSON;
const msdata = require('./js/msdata');

const extract_changed_keys = function(event) {
  if ( ! event.Records ) {
    return [];
  }
  let results = event.Records
  .filter( rec => rec.Sns )
  .map( rec => {
    let sns_message = JSON.parse(rec.Sns.Message);
    console.log(sns_message);
    throw new Error('Need to parse message format');
  });
  results = [].concat.apply([],results);
  return results.filter( obj => obj.bucket == bucket_name ).map( obj => obj.key );
};

// const retrieve_file_s3 = function retrieve_file_s3(filekey,byte_offset) {
//   let params = {
//     'Key' : filekey,
//     'Bucket' : bucket_name
//   };
//   if (byte_offset) {
//     params.Range = 'bytes='+byte_offset+'-';
//   }
//   let request = s3.getObject(params);
//   let stream = request.createReadStream();
//   return stream;
// };

const retrieve_file_local = function retrieve_file_local(filekey) {
  return fs.createReadStream(filekey);
};

// const retrieve_file = function retrieve_file(filekey,md5_result,byte_offset) {
//   return retrieve_file_s3(filekey,md5_result,byte_offset);
// };

const read_data_stream = function(path) {
  let input_stream = retrieve_file_local(path);
  let entry_data = input_stream.pipe(JSONStream.parse(['data', {'emitKey': true}]));
  return entry_data;
};

const write_frame_stream = function(json_stream) {
  let typeinfo =  {   'type': 'dataframe',
            'keys' : json_stream.keys,
            'types' : json_stream.types
          };

  Object.keys(json_stream.annotations).forEach( attribute => {
    let outstream = new PassThrough();
    let instream = new PassThrough({objectMode: true});
    let transformer = new RData(outstream);
    let attr_typeinfo = json_stream.annotations[attribute];
    json_stream.annotations[attribute] = instream;
    transformer.dataFrame(instream,attr_typeinfo.keys,attr_typeinfo.types,{}).then( () => transformer.finish() );
    if (! typeinfo.attributes ) {
      typeinfo.attributes = { values : {}, names : [], types: [] };
    }
    typeinfo.attributes.values[attribute] = outstream;
    typeinfo.attributes.names.push(attribute);
    typeinfo.attributes.types.push({'type' : 'dataframe'});
    json_stream.on('end', () => {
      console.log("Ending attribute data frame");
      instream.end();
    });
  });

  let output = require('fs').createWriteStream('output.Rdata');

  let writer = new RData(output);

  // Write the header for the R data file format
  writer.writeHeader();

  // We need to write out the data frame into an environment
  return writer.environment( {'data' : json_stream },{'data' : typeinfo })
      .then( () => writer.finish() );
};


const do_transform = function(filename) {
  write_frame_stream(read_data_stream(filename).pipe(new ConvertJSON(msdata))).then( () => {
    console.log('All done');
  });
};

const serialiseDataset = function(event,context) {
  let changed_keys = extract_changed_keys(event);
  console.log(changed_keys);
  Promise.resolve().then( () => {
    context.succeed('OK');
  }).catch( err => {
    console.error(err);
    console.error(err.stack);
    context.fail('NOT-OK');
  });
};

exports.serialiseDataset = serialiseDataset;
exports.do_transform = do_transform;