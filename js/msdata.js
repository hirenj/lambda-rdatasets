'use strict';
/*jshint esversion: 6, node:true */

const transformer = function(chunk) {
  let key = chunk.key;
  chunk.value.forEach( val => {
    let result = {};
    result.uniprot = key;
    result['peptide'] = val.sequence || null;
    result['peptide.start'] = val.peptide_start
    result['peptide.end'] = val.peptide_start + val.sequence.length - 1;
    result['source'] = val.source || null;
    result['quantification'] = (val.quant || {}).quant || null;
    result['quantification.channels'] = (val.quant || {}).channels || null;
    result['site.ambiguity'] = val.made_ambiguous || null;
    result['quantifiation.confidence'] = (val.quant || {}).singlet_confidence || null;
    result['composition'] = (val.composition || [])[0] || null;
    result['activation'] = (val.activation || []).join(',');

    val.sites.forEach( (site) => {
      let site_result = Object.assign({},result);
      site_result.site = site[0];
      site_result['site.composition'] = site[1];
      this.push(site_result);
    });
    val.sites_ambiguous.forEach( (site) => {
      let site_result = Object.assign({},result);
      site_result['site.ambiguous.start'] = site[0][0];
      site_result['site.ambiguous.end'] = site[0][1];
      site_result['site.composition'] = site[1];
      this.push(site_result);
    });
  });
};

transformer.types = [ 'string',
                      'string',
                      'int',
                      'int',
                      'int',
                      'int',
                      'int',
                      'string',
                      'string',
                      'real',
                      'string',
                      'string',
                      'string',
                      'string',
                      'string'
                    ];
transformer.keys = [  'uniprot',
                      'peptide',
                      'peptide.start',
                      'peptide.end',
                      'site',
                      'site.ambiguous.start',
                      'site.ambiguous.end',
                      'site.composition',
                      'source',
                      'quantification',
                      'quantification.channels',
                      'site.ambiguity',
                      'quantification.confidence',
                      'composition',
                      'activation'];

module.exports = transformer;