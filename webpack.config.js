const base_config = require('gator-webpack');
let config = base_config.Load();
const ModuleInstaller = base_config.ModuleInstaller;

config.plugins = [ new ModuleInstaller(['hirenj/node-uberon-mappings']) ].concat(config.plugins);

config.externals.concat(
  [{ 'node-uberon-mappings' : 'node-uberon-mappings' }
  ]
)
module.exports = config