const logger = require('pelias-logger').get( 'csv-importer');
const recordStream = require('./streams/recordStream');
const model = require('pelias-model');
const peliasDbclient = require('pelias-dbclient');
const blacklistStream = require('pelias-blacklist-stream');
const adminLookup = require('pelias-wof-admin-lookup');

const through = require('through2');
const DUMP_TO = process.env.DUMP_TO;

function createDocumentMapperStream() {
  if(DUMP_TO) {
    return through.obj( function( model, enc, next ){
      next(null, model.callPostProcessingScripts());
    });
  }

  return model.createDocumentMapperStream();
}

function createFullImportPipeline( files, dirPath, importerName ){
  logger.info( 'Importing %s files.', files.length );

  recordStream.create(files, dirPath)
    .pipe(blacklistStream())
    .pipe(adminLookup.create())
    .pipe(createDocumentMapperStream())
    .pipe(peliasDbclient({name: importerName}));
}

module.exports = {
  create: createFullImportPipeline
};
