'use strict';
/*jshint esversion: 6, node:true */

let total = 0;

const transformer = function(chunk,metadata) {
  let key = chunk.key;
  total = total + 1;

  chunk.value.forEach( val => {
    let result = {};
    result.gene_id = key;
    result.location_id = metadata.locations[val.loc].ontology_id;
    result.tissue = metadata.locations[val.loc].simple_tissue;
    result.simple_uberon = metadata.locations[val.loc].simple_uberon;
    result.location = metadata.locations[val.loc].description;
    result.expression = val.exp;
    this.push(result);
    this.annotations['quantiles'].push({ '0':val.annotation.exp[0], '25':val.annotation.exp[1],'50':val.annotation.exp[2],'75':val.annotation.exp[3],'100':val.annotation.exp[4] });
  });
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

transformer.annotations = {
  'quantiles' : {
    'type' : 'dataframe',
    'keys' : ['0','25','50','75','100'],
    'types' : ['real','real','real','real','real']
  }
};

module.exports = transformer;