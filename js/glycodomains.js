'use strict';
/*jshint esversion: 6, node:true */

let total = 0;

const transformer = function(chunk) {
  let key = chunk.key;
  total = total + 1;
  chunk.value.forEach( val => {
    let result = {};
    result.uniprot = key;
    result.start = val.start;
    result.end = val.end;
    result.typeid = val.interpro;
    result.class = (val.class || []).join(',');
    this.push(result);
  });
};

transformer.types = [ 'string',
                      'real',
                      'real',
                      'string',
                      'string'
                    ];
transformer.keys = [  'uniprot',
                      'start',
                      'end',
                      'typeid',
                      'class'
                    ];

transformer.annotations = {};

module.exports = transformer;