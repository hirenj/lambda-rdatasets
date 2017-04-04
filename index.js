'use strict';
/*jshint esversion: 6, node:true */

const PassThrough = require('stream').PassThrough;

let config = {};

let bucket_name = 'data';
let data_table = 'data';

try {
  config = require('./resources.conf.json');
  bucket_name = config.buckets.dataBucket;
  data_table = config.tables.data;
} catch (e) {
}

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
}

const RData = require('node-rdata');
const ConvertJSON = require('./js/transform').ConvertJSON;
const msdata = require('./js/msdata');
const expression = require('./js/expression');
const associations = require('./js/associations');
const jsonstreamer = require('node-jsonpath-s3');
const metaConverter = require('node-uberon-mappings');
const zlib = require('zlib');
const temp = require('temp');
const AWS = require('lambda-helpers').AWS;
const fs = require('fs');
const archiver = require('archiver');
const path = require('path');
const dynamo = new AWS.DynamoDB.DocumentClient();

const choose_transform = function(metadata) {
  if (metadata.mimetype == 'application/json+msdata') {
    return msdata;
  }
  if (metadata.mimetype == 'application/json+expression') {
    return expression;
  }
  if (metadata.mimetype == 'application/json+association') {
    return associations;
  }
};

const update_metadata = function(metadata) {
  if ( ! metadata.sample || ! metadata.sample.tissue ) {
    return;
  }
  return metaConverter.convert( metadata.sample.tissue ).then( converted => {
    if ( ! converted.root ) {
      return;
    }
    metadata.sample.uberon = converted.root;
    metadata.sample.description = converted.name;
  });
};

const extract_changed_keys = function(event) {
  if ( ! event.Records ) {
    if (event.Key) {
      return [event.Key];
    } else {
      return [];
    }
  }
  let results = event.Records
  .filter( rec => rec.Sns )
  .map( rec => {
    let sns_message = JSON.parse(rec.Sns.Message);
    return { bucket: sns_message.Bucket, key: sns_message.Key };
  });
  results = [].concat.apply([],results);
  return results.filter( obj => obj.bucket == bucket_name ).map( obj => obj.key );
};

const retrieve_file = function retrieve_file(path) {
  return jsonstreamer.getDataStream(path);
};

const retrieve_metadata = function retrieve_metadata(path) {
  return jsonstreamer.getMetadataStream(path);
};

const derive_basename = function(file_path) {
  if (file_path.indexOf('s3://') === 0) {
    return file_path.split('/')[1];
  }
  return path.basename(file_path).replace(/\.json$/,'').replace(/.msdata$/,'');
};

const get_file_data = function(path,metadata) {
  if ( ! metadata ) {
    let metadata_stream = retrieve_metadata(path);
    let retrieved = {};
    metadata_stream.on('data', meta => retrieved.data = meta );
    return metadata_stream.finished
    .then( () => update_metadata(retrieved.data))
    .then( () => get_file_data(path,retrieved.data) );
  }
  metadata.path_basename = derive_basename(path);
  let entry_data = retrieve_file(path);
  return Promise.resolve({ stream:entry_data, metadata:metadata });
};

const write_frame_stream = function(json_stream,metadata) {

  let title = metadata.title || metadata.path_basename || 'data';
  title = title.replace(/[^A-Za-z0-9]/g,'.').replace(/\.+/,'.').replace(/\.$/,'').replace(/^[0-9\.]+/,'');

  let typeinfo =  {
            'type': 'dataframe',
            'keys' : json_stream.keys,
            'types' : json_stream.types,
            'attributes' : { values: {
                                'taxon' : [metadata.sample.species],
                                'tissue' : [metadata.sample.tissue],
                                'basic_tissue' : [metadata.sample.description || ''],
                                'basic_uberon' : [metadata.sample.uberon || ''],
                                'celltype' : [metadata.sample.cell_type || ''],
                                'celltype.id' : [metadata.sample.cell_type_id || ''],
                                'title' : [title]
                              },
                             names: ['taxon','tissue','basic_tissue','basic_uberon','celltype','celltype.id','title'],
                             types: ['real','string','string','string','string','string','string']
                           }
          };

  ['ko','wt'].forEach(condition => {
    if (metadata.sample[condition] && metadata.sample[condition].length > 0) {
      typeinfo.attributes.values[condition+'.genes'] = metadata.sample[condition];
      typeinfo.attributes.names.push(condition+'.genes');
      typeinfo.attributes.types.push('string');
    }
  });

  Object.keys((metadata.quantitation || {}).channels || {}).forEach(channel => {
    typeinfo.attributes.values['channel.sample.'+channel] = [ metadata.quantitation.channels[channel] ];
    typeinfo.attributes.names.push('channel.sample.'+channel);
    typeinfo.attributes.types.push('string');
  });


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
      instream.end();
    });
  });

  let gz = zlib.createGzip();

  let writer = new RData(gz);


  let outstream = temp.createWriteStream();
  let output_path = outstream.path;

  gz.pipe(outstream);

  // Write the header for the R data file format
  writer.writeHeader();

  // We need to write out the data frame into an environment

  let stream_block = {};
  let type_block = {};
  stream_block[title] = json_stream;
  type_block[title] = typeinfo;
  return writer.environment( stream_block,type_block )
      .then( () => writer.finish() )
      .then( () => { return { path: output_path, metadata: metadata, title: title }; });
};


const do_transform = function(filename,metadata) {
  return get_file_data(filename,metadata).then( streaminfo => {
    let stream = streaminfo.stream;
    let metadata = streaminfo.metadata;
    let transformer = choose_transform(metadata);
    return write_frame_stream( stream.pipe(new ConvertJSON(transformer)), metadata );
  })
  .catch( err => { console.log(err); console.log(err.stack); });
};

const generate_description = function(filedata) {
  let title = filedata.title;
  let now = new Date().toISOString().split('T')[0];
  let version = now.replace(/-/g,'.');
  filedata.version = version;
  let date = now;
  let description = `\
Package: gator.${title}
Version: ${version}
Date: ${date}
Depends: R (>= 3.1.0)
Description: ${title}
Title: ${title}
LazyData: yes
NeedsCompilation: yes`;
  return description;
};

const create_package = function(filedata) {
  let gz = zlib.createGzip();
  let archive = archiver('tar', { store: true });
  archive.pipe(gz);
  archive.append(fs.createReadStream(filedata.path), { name: `${filedata.title}/data/data.rda` });
  archive.append('', { name: `${filedata.title}/NAMESPACE` });
  archive.append(generate_description(filedata),{ name: `${filedata.title}/DESCRIPTION` });
  archive.finalize();
  return gz;
};


const uploadToS3 = function(target) {
  var pass = new PassThrough();

  var params = {Bucket: bucket_name, Key: target, Body: pass};
  let s3 = new AWS.S3();
  pass.finished = new Promise( (resolve,reject) => {
    s3.upload(params, function(err, data) {
      if (err) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
  return pass;
};

const transformDataS3 = function(input_key,target_prefix) {
  return do_transform(input_key).then( filedata => {
    let package_stream = create_package(filedata);
    target_prefix = target_prefix || '';
    let output_pipe = uploadToS3(`${target_prefix}${filedata.title}_${filedata.version}`);
    package_stream.pipe(output_pipe);
    return output_pipe.finished.then( () => filedata );
  });
};

const transformDataLocal = function(input_key) {
  return do_transform(input_key).then( filedata => {
    let package_stream = create_package(filedata);
    let output_pipe = fs.createWriteStream(`${filedata.title}_${filedata.version}.tar.gz`);
    package_stream.pipe(output_pipe);
    return new Promise( (resolve,reject) => {
      output_pipe.on('finish',resolve);
      output_pipe.on('error',reject);
      package_stream.on('error',reject);
    }).then( () => filedata );
  });
};

var write_metadata = function write_metadata(set_id,path) {
  let params = {
   'TableName' : data_table,
   'Key' : {'acc' : 'metadata', 'dataset' : set_id }
  };
  params.UpdateExpression = 'SET #rdata = :path';
  params.ExpressionAttributeValues = {
      ':path': path,
  };
  params.ExpressionAttributeNames = {
    '#rdata' : 'rdata_file'
  };
  console.log('Setting RData for',set_id,'to',path);
  return dynamo.update(params).promise();
};

const serialiseDataset = function(event,context) {
  let changed_keys = extract_changed_keys(event);
  if (changed_keys.length < 1) {
    context.succeed('OK');
    return;
  }
  console.log(changed_keys);
  let key = changed_keys[0];
  let output_key = key.split('/')[1];
  if ( ! output_key ) {
    console.log('Missing output key for',key);
    context.fail('NOT-OK');
    return;
  }
  console.log('Transforming from',`s3://${bucket_name}/${key}`,'to (approximately)',`rdata/${output_key}_1970.01.01`);
  transformDataS3(`s3://${bucket_name}/${key}`,'rdata/')
  .then( (filedata) => write_metadata(output_key,`${filedata.title}_${filedata.version}`))
  .then( () => context.succeed('OK') )
  .catch( err => {
    console.error(err);
    console.error(err.stack);
    context.fail('NOT-OK');
  });
};

exports.serialiseDataset = serialiseDataset;
exports.do_transform = transformDataLocal;
exports.do_transform_s3 = transformDataS3;