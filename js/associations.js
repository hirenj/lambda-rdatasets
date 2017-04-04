'use strict';
/*jshint esversion: 6, node:true */

let total = 0;

const transformer = function(chunk) {
  let key = chunk.key;
  total = total + 1;
  chunk.value.forEach( val => {
    let result = {};
    result.gene_id = key;
    result.chromosome = val.chr;
    result.position = val.pos;
    result.pvalue = val['p-value'];
    result.snp = 'rs'+val['snp'];
    result.efo = val.trait_uri.split('/').reverse()[0];
    result.trait = val.trait;

    if (val.snp_position == 'upstream') {
      result.gene_distance = Math.abs(val['distance']);
    } else {
      result.gene_distance = 0;
    }
    if (val.snp_position) {
      result.gene_position = val.snp_position;
      result.intergenic = true;
    }
    this.push(result);
  });
};

transformer.types = [ 'string',
                      'string',
                      'real',
                      'real',
                      'string',
                      'string',
                      'string',
                      'real',
                      'string',
                      'logical'
                    ];
transformer.keys = [  'gene_id',
                      'chromosome',
                      'position',
                      'pvalue',
                      'snp',
                      'efo',
                      'trait',
                      'gene_distance',
                      'gene_position',
                      'intergenic'
                    ];

transformer.annotations = {};

module.exports = transformer;