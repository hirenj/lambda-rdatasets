'use strict';
/*jshint esversion: 6, node:true */

let total = 0;

const transformer = function(chunk) {
  let key = chunk.key;
  total = total + 1;
  chunk.value.forEach( val => {
    let result = {};
    result.gene_id = key;
    result.uberon = val.uberon;
    result.tissue = val.simple_tissue;
    result.simple_uberon = val.simple_uberon;
    result.expression = val.exp;
    this.push(result);
    this.annotations['quantiles'].push({ '0':val.annotation.exp[0], '25':val.annotation.exp[1],'50':val.annotation.exp[2],'75':val.annotation.exp[3],'100':val.annotation.exp[4] });
  });
};

transformer.types = [ 'string',
                      'string',
                      'string',
                      'string',
                      'real'
                    ];
transformer.keys = [  'gene_id',
                      'uberon',
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