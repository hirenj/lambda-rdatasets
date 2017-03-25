'use strict';
/*jshint esversion: 6, node:true */

const Transform = require('stream').Transform;
const util = require('util');

const ConvertJSON = function(func) {
  this.transformer = func;
  this.annotations = {};
  Object.keys(func.annotations || {}).forEach( attr => {
    this.annotations[attr] = func.annotations[attr];
  });
  this.types = func.types;
  this.keys = func.keys;
  Transform.call(this, {objectMode: true});
};

util.inherits(ConvertJSON, Transform);

ConvertJSON.prototype._transform = function(chunk,enc,cb) {
  let self = this;
  this.transformer(chunk);
  cb();
};

exports.ConvertJSON = ConvertJSON;