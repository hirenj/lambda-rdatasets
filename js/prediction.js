'use strict';
/*jshint esversion: 6, node:true */

const transformer = function(chunk) {
  let key = chunk.key;
  chunk.value.forEach( val => {
    val.sites.forEach( (site,idx) => {
      let result = {};
      result['uniprot'] = key;
      result['site'] = site[0];
      result['site.composition'] = site[1];
      result['score'] = null;
      if (val.annotations && val.annotations['score']) {
        result['score'] = val.annotations['score'][idx];
      }
      this.push(result);
    });
  });
};

transformer.types = [ 'string',
                      'int',
                      'string',
                      'real'
                    ];
transformer.keys = [  'uniprot',
                      'site',
                      'site.composition',
                      'score'
                    ];

transformer.annotations = {};

module.exports = transformer;