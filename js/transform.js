'use strict';
/*jshint esversion: 6, node:true */

const Transform = require('stream').Transform;
const util = require('util');

const seedrandom = require('seedrandom');


const ConvertJSON = function(func,metadata,sample) {
  this.transformer = func;
  this.metadata = metadata;
  this.annotations = {};
  Object.keys(func.annotations || {}).forEach( attr => {
    this.annotations[attr] = func.annotations[attr];
  });
  this.types = func.types;
  this.keys = func.keys;
  if (sample && sample.rate) {
    this.sample = sample.rate;
    this.rng = seedrandom(sample.seed || new Date().getTime());
  }
  Transform.call(this, {objectMode: true});
};

util.inherits(ConvertJSON, Transform);

ConvertJSON.prototype._transform = function(chunk,enc,cb) {
  let self = this;
  if (! this.sample || (this.rng() <= this.sample) ){
    this.transformer(chunk,this.metadata);
  }
  cb();
};

exports.ConvertJSON = ConvertJSON;