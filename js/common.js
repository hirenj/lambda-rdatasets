'use strict';
/*jshint esversion: 6, node:true */

const PassThrough = require('stream').PassThrough;

let config = {};

let bucket_name = process.env.BUILD_BUCKET || 'data';
let data_table = process.env.BUILD_TABLE || 'data';
let stack;

try {
  config = require('../resources.conf.json');
  bucket_name = config.buckets.dataBucket;
  data_table = config.tables.data;
  stack = config.stack;
} catch (e) {
}

if (config.region) {
  require('lambda-helpers').AWS.setRegion(config.region);
}

const RData = require('node-rdata');

const ConvertJSON = require('./transform').ConvertJSON;
const msdata = require('./msdata');
const expression = require('./expression');
const prediction = require('./prediction');
const expression_slim = require('./expression_slim');
const associations = require('./associations');
const glycodomains = require('./glycodomains');
const jsonstreamer = require('node-jsonpath-s3');
const convert = require('node-uberon-mappings').convert;
const temp = require('temp');
const AWS = require('lambda-helpers').AWS;
const fs = require('fs');
const path = require('path');
const dynamo = new AWS.DynamoDB.DocumentClient();
const codebuild = new AWS.CodeBuild();

const MAX_FILE_SIZE = 50*1024*1024;

const choose_transform = function(metadata) {
  if (metadata.mimetype == 'application/json+msdata') {
    if (metadata['msdata-version']) {
      return msdata.version(metadata['msdata-version']);
    }
    return msdata.latest();
  }
  if (metadata.mimetype == 'application/json+expression') {
    return expression;
  }
  if (metadata.mimetype == 'application/json+msdata-prediction') {
    return prediction;
  }
  if (metadata.mimetype == 'application/json+slim_expression') {
    return expression_slim;
  }
  if (metadata.mimetype == 'application/json+association') {
    return associations;
  }
  if (metadata.mimetype == 'application/json+glycodomain') {
    return glycodomains;
  }
};

const update_metadata = function(metadata) {
  if ( ! metadata.sample || ! metadata.sample.tissue ) {
    return;
  }
  return convert( metadata.sample.tissue ).then( converted => {
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
      return [ event ];
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

const start_build = function(key,serialiser) {
  return codebuild.startBuild({
    projectName: stack+'-SerialiseDatasetBuild',
    timeoutInMinutesOverride: 60,
    environmentVariablesOverride: [
    {
      name: 'BUILD_KEY',
      value: key
    },
    {
      name: 'BUILD_SERIALISER',
      value: serialiser
    }]
  }).promise();
};

const long_build_if_needed = function long_build_if_needed(bucket,key,serialiser) {
  let params = {
    Bucket: bucket,
    Key: key
  };
  let s3 = new AWS.S3();
  return s3.headObject(params).promise().then( head => {
    if (head.ContentLength >= MAX_FILE_SIZE) {
      return start_build(key,serialiser).then( () => {
        throw new Error('Using long build');
      });
    }
    return true;
  });
};

const retrieve_file = function retrieve_file(path) {
  return jsonstreamer.getDataStream(path);
};

const retrieve_metadata = function retrieve_metadata(path,offset) {
  return jsonstreamer.getMetadataStream(path,offset);
};

const derive_basename = function(file_path) {
  if (file_path.indexOf('s3://') === 0) {
    return file_path.split('/')[1];
  }
  return path.basename(file_path).replace(/\.json$/,'').replace(/.msdata$/,'');
};

const get_file_data = function(path,metadata) {
  if ( ! metadata ) {
    return retrieve_metadata(path,-1024*1024).then( metadata_stream => {
      let retrieved = {};
      metadata_stream.on('data', meta => retrieved.data = meta );
      return metadata_stream.finished
      .then( () => update_metadata(retrieved.data))
      .then( () => get_file_data(path,retrieved.data) );
    });
  }
  metadata.path_basename = derive_basename(path);
  let entry_data = retrieve_file(path);
  return entry_data.then( stream => { return { stream: stream, metadata: metadata }; });
};

const write_frame_stream = function(serializer,json_stream,metadata) {

  let title = metadata.title || metadata.path_basename || 'data';
  title = title.replace(/[^A-Za-z0-9]/g,'.').replace(/\.+/,'.').replace(/\.$/,'').replace(/^[0-9\.]+/,'');

  if ( ! metadata.sample ) {
    metadata.sample = {};
  }
  if (metadata.taxonomy && ! metadata.sample.species) {
    metadata.sample.species = parseInt(metadata.taxonomy);
  }

  let typeinfo =  {
            'type': 'dataframe',
            'keys' : json_stream.keys,
            'types' : json_stream.types,
            'attributes' : { values: {
                                'taxon' : [metadata.sample.species || ''],
                                'tissue' : [metadata.sample.tissue || ''],
                                'basic_tissue' : [metadata.sample.description || ''],
                                'basic_uberon' : [metadata.sample.uberon || ''],
                                'celltype' : [metadata.sample.cell_type || ''],
                                'celltype.id' : [metadata.sample.cell_type_id || ''],
                                'title' : [title],
                                'type' : [(metadata.mimetype || '').replace('application/json+','')],
                                'msdata.version' : [metadata['msdata-version']],
                                'doi' : [ metadata.doi || '' ]
                              },
                             names: ['taxon','tissue','basic_tissue','basic_uberon','celltype','celltype.id','title','type','msdata.version','doi'],
                             types: ['real','string','string','string','string','string','string','string','string','string']
                           }
          };

  ['ko','wt'].forEach(condition => {
    if (metadata.sample[condition] && metadata.sample[condition].length > 0) {
      typeinfo.attributes.values[condition+'.genes'] = metadata.sample[condition];
      typeinfo.attributes.names.push(condition+'.genes');
      typeinfo.attributes.types.push('string');
    }
  });

  for (let [channel,channel_sample] of Object.entries((metadata.quantitation || {}).channels || {})) {
    typeinfo.attributes.values['channel.sample.'+channel] = [ channel_sample ];
    typeinfo.attributes.names.push('channel.sample.'+channel);
    typeinfo.attributes.types.push('string');
  }

  if (metadata.channel_samples) {
    json_stream.annotations['channel.samples'] =
    {
      'type': 'dataframe',
      'keys' : ['channel','sample','type','entrez','symbol'],
      'types' : ['string','string','string','int','string']
    };
  }


  Object.keys(json_stream.annotations).forEach( attribute => {
    let outstream = new PassThrough();
    let instream = new PassThrough({objectMode: true});
    let transformer = new serializer(outstream);
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


  if (metadata.channel_samples && metadata.quantitation) {

    let sample_infos = Object.entries(metadata.quantitation.channels || {})
    .map( ([channel,sample]) =>{ return { channel, sample }; } );

    sample_infos.forEach( (channel_info) => channel_info.sample_info = metadata.channel_samples[channel_info.sample] );

    for (let {channel,sample,sample_info} of sample_infos ) {
      let ko = sample_info['perturbation-ko'];
      let ki = sample_info['perturbation-ki'];
      let wt = sample_info['perturbation-wt'];
      let other = sample_info['perturbation-other'];

      for (let {entrez,symbol} of ko ) {
        json_stream.annotations['channel.samples'].push( {
          channel,sample, type: "ko", entrez, symbol
        });
      }

      for (let {entrez,symbol} of ki ) {
        json_stream.annotations['channel.samples'].push( {
          channel,sample, type: "ki", entrez, symbol
        });
      }

      for (let {entrez,symbol} of wt ) {
        json_stream.annotations['channel.samples'].push( {
          channel,sample, type: "wt", entrez, symbol
        });
      }

      for (let other_desc of other ) {
        json_stream.annotations['channel.samples'].push( {
          channel,sample, type: other_desc
        });
      }

    }
      
  }


  let outstream = temp.createWriteStream();
  let output_path = outstream.path;

  let writer = new serializer(outstream, {gzip: true});

  // Write the header for the R data file format
  writer.writeHeader();

  // We need to write out the data frame into an environment

  let stream_block = {};
  let type_block = {};
  stream_block[title] = json_stream;
  type_block[title] = typeinfo;
  return writer.environment( stream_block,type_block )
      .then( () => writer.finish() )
      .then( () => { return { path: output_path, metadata: metadata, title: title, suffix: serializer.suffix }; });
};


const perform_transform = function(serializer,filename,metadata,sample) {
  return get_file_data(filename,metadata).then( streaminfo => {
    let stream = streaminfo.stream;
    let metadata = streaminfo.metadata;
    let transformer = choose_transform(metadata);
    if ( ! transformer ) {
      throw new Error('No transformer');
    }
    let conversion_pipe = stream.pipe(new ConvertJSON(transformer,metadata,sample));
    if (serializer.Formatter) {
      let formatter_class = serializer.Formatter;
      conversion_pipe = conversion_pipe.pipe(new formatter_class(metadata));
    }
    return write_frame_stream( serializer, conversion_pipe, metadata );
  })
  .catch( err => {
    if (err.message === 'No transformer') {
      throw err;
    }
    console.log(err);
    console.log(err.stack);
  });
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

const version_filedata = function(filedata) {
  let now = new Date().toISOString().split('T')[0];
  let version = now.replace(/-/g,'.');
  filedata.version = version;
  return filedata;
};

const transformDataS3 = function(transformer,input_key,target_prefix,metadata) {
  let sample;

  if ( ! transformer ) {
    transformer = RData;
  }
  if (transformer === 'RData') {
    transformer = RData;
  }

  if (transformer === 'RData_partial') {
    transformer = RData;
    sample = { rate: 0.05, seed: 'SEED' };
  }

  return perform_transform(transformer,input_key,metadata,sample).then( filedata => {
    version_filedata(filedata);
    let package_stream = transformer.package(filedata, { prefix: 'gator', data_filename: 'data' });
    target_prefix = target_prefix || '';
    let suffix = '';
    if (filedata.suffix) {
      suffix = `.${filedata.suffix}`;
    }
    let filename = filedata.title;
    if (sample) {
      filename = `partial_${filename}`;
    }
    let output_pipe = uploadToS3(`${target_prefix}${filename}_${filedata.version}${suffix}`);
    package_stream.pipe(output_pipe);
    return output_pipe.finished.then( () => filedata );
  });
};

const transformDataLocal = function(transformer,input_key) {
  let sample;

  if ( ! transformer ) {
    transformer = RData;
  }
  if (transformer === 'RData') {
    transformer = RData;
  }

  if (transformer === 'RData_partial') {
    transformer = RData;
    sample = { rate: 0.05, seed: 'SEED' };
  }

  return perform_transform(transformer,input_key,null,sample).then( filedata => {
    version_filedata(filedata);
    let package_stream = transformer.package(filedata, { prefix: 'gator', data_filename: 'data' });
    let suffix = '';
    if (filedata.suffix) {
      suffix = `.${filedata.suffix}`;
    }
    let filename = filedata.title;
    if (sample) {
      filename = `partial_${filename}`;
    }
    let output_pipe = fs.createWriteStream(`${filename}_${filedata.version}${suffix}`);
    package_stream.pipe(output_pipe);
    return new Promise( (resolve,reject) => {
      output_pipe.on('finish',resolve);
      output_pipe.on('error',reject);
      package_stream.on('error',reject);
    }).then( () => filedata );
  });
};

var write_metadata = function write_metadata(set_id,path) {
  console.log('Updating Rdata path for ',set_id,path);
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
  console.log('Setting data file path for',set_id,'to',path);
  return dynamo.update(params).promise();
};

const serialiseDataset = function(event,context) {
  let changed_keys = extract_changed_keys(event);
  if (changed_keys.length < 1) {
    context.succeed('OK');
    return;
  }
  console.log(changed_keys);
  let key = changed_keys.map( entry => (typeof entry === 'object') ? entry.Key: entry )[0];
  let metadata = changed_keys.map( entry => entry.metadata )[0];
  let output_key = key.split('/')[1];
  if ( ! output_key ) {
    console.log('Missing output key for',key);
    context.fail('NOT-OK');
    return;
  }

  console.log('Transforming from',`s3://${bucket_name}/${key}`,'to (approximately)',`rdata/${output_key}_1970.01.01`,event.serialiser);
  long_build_if_needed(bucket_name,key,event.serialiser)
  .then( () => transformDataS3(event.serialiser,`s3://${bucket_name}/${key}`,'rdata/',metadata))
  .then( (filedata) => write_metadata(output_key,`${filedata.title}_${filedata.version}`))
  .then( () => context.succeed('OK') )
  .catch( err => {
    if (err.message == 'No transformer') {
      console.log('No transformer for mimetype',metadata.mimetype,'skipping');
      context.succeed('OK');
      return;
    }
    if (err.message == 'Using long build') {
      console.log('Using long build');
      context.succeed('OK');
    }
    console.error(err);
    console.error(err.stack);
    context.fail('NOT-OK');
  });
};

exports.serialiseDataset = serialiseDataset;
exports.transformers = [ RData ];
exports.do_transform = transformDataLocal;
exports.do_transform_s3 = transformDataS3;
exports.update_metadata = write_metadata;