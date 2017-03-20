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
    val.sites.forEach( (site) => {
      let site_result = Object.assign({},result);
    });
    // Loop over sites..
    this.push(result);
  });
};

transformer.types = ['string','string','int','int','string','real','string'];
transformer.keys = ['uniprot','peptide','peptide.start','peptide.end','site_ambiguity','quantification','quant_confidence'];

module.exports = transformer;