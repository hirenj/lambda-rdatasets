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
const RData = require('node-rdata');
const ConvertJSON = require('./js/transform').ConvertJSON;
const msdata = require('./js/msdata');
const jsonstreamer = require('node-jsonpath-s3');
const metaConverter = require('node-uberon-mappings');

const choose_transform = function(metadata) {
  if (metadata.mimetype == 'application/json+msdata') {
    return msdata;
  }
};

const update_metadata = function(metadata) {
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

const retrieve_file = function retrieve_file(path) {
  return jsonstreamer.getDataStream(path);
};

const retrieve_metadata = function retrieve_file(path) {
  return jsonstreamer.getMetadataStream(path);
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
  let entry_data = retrieve_file(path);
  return Promise.resolve({ stream:entry_data, metadata:metadata });
};

const write_frame_stream = function(json_stream,metadata) {
  let typeinfo =  {
            'type': 'dataframe',
            'keys' : json_stream.keys,
            'types' : json_stream.types,
            'attributes' : { values: {
                                'taxon' : [metadata.sample.species],
                                'tissue' : [metadata.sample.tissue],
                                'basic_tissue' : [metadata.sample.description],
                                'basic_uberon' : [metadata.sample.uberon],
                                'celltype' : [metadata.sample.cell_type || ''],
                                'celltype.id' : [metadata.sample.cell_type_id || '']
                              },
                             names: ['taxon','tissue','basic_tissue','basic_uberon','celltype','celltype.id'],
                             types: ['int','string','string','string','string','string']
                           }
          };

  ['ko','wt'].forEach(condition => {
    if (metadata.sample[condition]) {
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

  let output = require('fs').createWriteStream('output.Rdata');

  let writer = new RData(output);

  // Write the header for the R data file format
  writer.writeHeader();

  // We need to write out the data frame into an environment
  return writer.environment( {'data' : json_stream },{'data' : typeinfo })
      .then( () => writer.finish() );
};


const do_transform = function(filename,metadata) {
  return get_file_data(filename,metadata).then( streaminfo => {
    let stream = streaminfo.stream;
    let metadata = streaminfo.metadata;
    let transformer = choose_transform(metadata);
    return write_frame_stream( stream.pipe(new ConvertJSON(transformer)), metadata );
  })
  .then( () => {
    console.log('All done');
  })
  .catch( console.log.bind(console) );
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