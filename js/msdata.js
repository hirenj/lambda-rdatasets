'use strict';
/*jshint esversion: 6, node:true */

const uuid = require('uuid');

const transformer = function(chunk) {
  let key = chunk.key;
  chunk.value.forEach( val => {
    let result = {};
    result.uniprot = key;
    let peptide_uuid = uuid.v4();
    result['peptide.id'] = peptide_uuid;
    result['peptide'] = val.sequence || null;
    result['peptide.start'] = val.peptide_start
    result['peptide.end'] = (val.peptide_start && val.sequence) ? (val.peptide_start + val.sequence.length - 1) : null;
    result['source'] = val.source || null;
    result['quantification'] = (val.quant || {}).quant || null;
    if (result['quantification']) {
      result['quantification'] = result['quantification'].toString();
    }
    if (val.quant && val.quant.areas) {
      Object.keys(val.quant.areas).forEach(channel => {
        [].concat(val.quant.areas[channel]).forEach( area => {
          this.annotations['quant.areas'].push({ 'peptide.id' : peptide_uuid, 'channel' : channel, 'area' : area });
        });
      });
    }
    result['quantification.channels'] = (val.quant || {}).channels || null;
    result['site.ambiguity'] = val.made_ambiguous || null;
    result['quantifiation.confidence'] = (val.quant || {}).singlet_confidence || null;
    result['composition'] = (val.composition || [])[0] || null;
    result['activation'] = (val.activation || []).join(',');

    (val.sites || []).forEach( (site) => {
      let site_result = Object.assign({},result);
      site_result.site = site[0];
      site_result['site.composition'] = site[1];
      this.push(site_result);
    });
    (val.ambiguous_sites || []).forEach( (site) => {
      let site_result = Object.assign({},result);
      site_result['site.ambiguous.start'] = site[0][0];
      site_result['site.ambiguous.end'] = site[0][1];
      site_result['site.composition'] = site[1];
      this.push(site_result);
    });
    (val.spectra || []).forEach( spec => {
      this.annotations['spectra'].push(Object.assign({ 'peptide.id' : peptide_uuid }, spec ));
    });
    if (val.annotations && val.annotations['hexnac_calls']) {
      this.annotations['hexnac_type'].push({ 'peptide.id' : peptide_uuid ,
                                             'hexnac.call' : val.annotations['hexnac_calls'][0],
                                             'hexnac.ratio' : +val.annotations['hexnac_ratios']
                                           });
    }
  });
};

transformer.types = [ 'string',
                      'string',
                      'string',
                      'int',
                      'int',
                      'int',
                      'int',
                      'int',
                      'string',
                      'string',
                      'string',
                      'string',
                      'string',
                      'string',
                      'string',
                      'string'
                    ];
transformer.keys = [  'uniprot',
                      'peptide.id',
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

transformer.annotations = {
  'spectra' : {
    'type' : 'dataframe',
    'keys' : ['peptide.id','score','rt','scan','ppm','mass','charge'],
    'types' : ['string','real','real','string','real','real','int']
  },
  'hexnac_type' : {
    'type' : 'dataframe',
    'keys' : ['peptide.id','hexnac.call','hexnac.ratio'],
    'types' : ['string','string','real']
  },
  'quant.areas' : {
    'type' : 'dataframe',
    'keys' : ['peptide.id','channel','area'],
    'types': ['string','string','real']
  }
};

module.exports = transformer;