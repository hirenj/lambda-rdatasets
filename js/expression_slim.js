'use strict';
/*jshint esversion: 6, node:true */

let total = 0;

Array.prototype.chunk = function ( n ) {
    if ( !this.length ) {
        return [];
    }
    return [ this.slice( 0, n ) ].concat( this.slice(n).chunk(n) );
};

const transformer = function(chunk,metadata) {
  let key = chunk.key;
  let idx = 0;
  total = total + 1;
  let values = chunk.value;
  for (idx=values.length-1; idx > 0 ; idx -= 2) {
    let loc = values[idx-1];
    let exp = values[idx];
    let result = {};
    result.gene_id = key;
    result.location_id = metadata.locations[loc].ontology_id;
    result.tissue = metadata.locations[loc].simple_tissue;
    result.simple_uberon = metadata.locations[loc].simple_uberon;
    result.location = metadata.locations[loc].description;
    result.expression = exp;
    this.push(result);
  };
};

transformer.types = [ 'string',
                      'string',
                      'string',
                      'string',
                      'string',
                      'real'
                    ];
transformer.keys = [  'gene_id',
                      'location_id',
                      'location',
                      'tissue',
                      'simple_uberon',
                      'expression'
                    ];

module.exports = transformer;