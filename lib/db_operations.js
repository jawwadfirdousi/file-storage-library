'use strict';

const url = require('url');

const _ = require('lodash');
const pg = require('pg');
const squelPostgres = require('squel').useFlavour('postgres');
const uuid = require('uuid');

// constructor with Connection String
function DbOperations (connectionOptions) {
  this.pool = null;

  if (connectionOptions) {
    this.setConnection(connectionOptions);
  }
}

DbOperations.prototype.setConnection = function setConnection (options) {
  let pool;

  if (!options) {
    throw new Error('No connection options were supplied');
  }

  // Postgres URL was supplied
  if (_.isString(options)) {
    const parsedUrl = url.parse(options);
    const auth = parsedUrl.auth.split(':');

    pool = new pg.Pool({
      user: auth[0],
      password: auth[1],
      host: parsedUrl.hostname,
      port: parsedUrl.port,
      database: parsedUrl.pathname.split('/')[1],
      max: 5,
      min: 2,
      idleTimeoutMillis: 5 * 60e3,
    });
  }

  // Direct pg Pool or utral instance
  if (options.query && _.isFunction(options.query)) {
    pool = options;
  }

  if (!pool) {
    throw new Error('Invalid options supplied for DbOperation connection');
  }

  this.pool = pool;
};

/**
 * Closes the underlying pg.Pool
 */
DbOperations.prototype.close = function () {
  this.pool.end();
};

/**
 * Executes query on Postgres
 * @param {pgUrl} query to execute
 * @param {string} query to execute
 * @param {arary} (optional) parameters query parameters in strict order
 * @param {Function} callback The completion handler called after the query is executed
 */
function executeQuery (pool, query, optParameters, callbackArg) {

  let parameters = optParameters;
  let callback = callbackArg;
  if (typeof parameters === 'function') {
    callback = parameters;
    parameters = null;
  }

  // Use Pool.query to automatically release clients
  pool.query(query, parameters, (queryErr, result) => {
    callback(queryErr, result);
  });
}

/**
 * Writes file metadata in master table
 * @param {Object} metadata serialized object
 * @param {Function} callback The completion handler called after the data has written
 */
DbOperations.prototype.writeMetadata = function writeMetadata (metadata, callback) {
  // FIXME: Change model to deduplicate actual bytes but allow multiple metadata definitions
  //        Metadata should be checked for equality (excluding recordDate)

  // Deduplicate by default
  const deduplicate = (metadata.deduplicate !== false);

  // TODO: make it pretty, please.
  const query = `insert into files (id, file_date, record_date, original_name,
    generated_name, mime_type, file_extension, source, file_sha256sum, chunk_size,
    file_type_hierarchy, file_size, attributes, status)
    values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)` +
    (deduplicate ?
      '\nON CONFLICT(file_sha256sum) DO UPDATE SET file_sha256sum = EXCLUDED.file_sha256sum\n' :
      // For salesforce file transfers.
      (metadata.id ?
        '\nON CONFLICT(id) DO UPDATE SET id = EXCLUDED.id\n' :
        ''
      )
    ) +
    '\nreturning *';

  const uuidForFile = metadata.id || uuid.v4();
  const parameters = [uuidForFile,
  metadata.fileDate,
  metadata.recordDate,
  metadata.originalName,
  metadata.generatedName,
  metadata.mimeType,
  metadata.fileExtension,
  metadata.fileSource,
  metadata.fileChecksum,
  metadata.fileChunkSize,
  metadata.fileHierarchy,
  metadata.fileSize,
  metadata.attributes,
  metadata.fileStatus,
  ];
  executeQuery(this.pool, query, parameters, (err, result) => {
    if (err) {
      callback(err);
      return;
    }
    callback(null, result.rows);
  });
};


/**
 * Updates file metadata
 * @param {Object} metadata serialized object
 * @param {Function} callback The completion handler called after the data has written
 */
DbOperations.prototype.updateMetadata = function updateMetadata (metadata, callback) {

  let updateQuery = squelPostgres.update()
  .table('files');

  if (metadata.fileDate) {
    updateQuery.set('file_date', metadata.fileDate);
  }
  if (metadata.recordDate) {
    updateQuery.set('record_date', metadata.recordDate);
  }
  if (metadata.originalName) {
    updateQuery.set('original_name', metadata.originalName);
  }
  if (metadata.generatedName) {
    updateQuery.set('generated_name', metadata.generatedName);
  }
  if (metadata.mimeType) {
    updateQuery.set('mime_type', metadata.mimeType);
  }
  if (metadata.fileExtension) {
    updateQuery.set('file_extension', metadata.fileExtension);
  }
  if (metadata.fileSource) {
    updateQuery.set('source', metadata.fileSource);
  }
  if (metadata.fileChecksum) {
    updateQuery.set('file_sha256sum', metadata.fileChecksum);
  }
  if (metadata.fileChunkSize) {
    updateQuery.set('chunk_size', metadata.fileChunkSize);
  }
  if (metadata.fileHierarchy) {
    updateQuery.set('file_type_hierarchy', `{${metadata.fileHierarchy.join()}}`);
  }
  if (metadata.fileSize) {
    updateQuery.set('file_size', metadata.fileSize);
  }
  if (metadata.attributes) {
    updateQuery.set('attributes', JSON.stringify(metadata.attributes));
  }
  if (metadata.fileStatus) {
    updateQuery.set('status', metadata.fileStatus);
  }
  updateQuery = updateQuery.where('id = ?', metadata.fileId)
  .returning('*')
  .toParam();

  executeQuery(this.pool, updateQuery, (err, result) => {
    if (err) {
      callback(err);
      return;
    }

    let updatedRow = null;
    if (result.rows.length > 0) {
      updatedRow = {
        fileId: result.rows[0].id,
        fileDate: result.rows[0].file_date,
        recordDate: result.rows[0].record_date,
        originalName: result.rows[0].original_name,
        generatedName: result.rows[0].generated_name,
        mimeType: result.rows[0].mime_type,
        fileExtension: result.rows[0].file_extension,
        fileSource: result.rows[0].source,
        fileChecksum: result.rows[0].file_sha256sum,
        fileChunkSize: result.rows[0].chunk_size,
        fileHierarchy: result.rows[0].file_type_hierarchy,
        fileSize: result.rows[0].file_size,
        attributes: result.rows[0].attributes,
        fileStatus: result.rows[0].status,
        numberOfChunks: Math.ceil(result.rows[0].file_size / result.rows[0].chunk_size),
      };
    }
    callback(null, updatedRow);
  });
};

/**
 * Writes file metadata in child table
 * @param {Object} chunk serialized chunk object
 * @param {string} metadataId Id of the file the chunk belongs to
 * @param {Function} callback The completion handler called after the data has written
 */
DbOperations.prototype.writeChunk = function writeChunk (chunk, callback) {
  // write chunk
  const parameters = [chunk.fileId, chunk.chunkNumber, chunk.chunkChecksum, chunk.data];
  const chunkQuery = squelPostgres.insert()
  .into('file_chunks')
  .set('file_id', '$1')
  .set('chunk_number', '$2')
  .set('chunk_sha256sum', '$3')
  .set('data', '$4')
  .toParam();
  executeQuery(this.pool, chunkQuery, parameters, callback);
};

/**
 * Updates file's metadata status
 * @param {string} fileId Id of the record to update
 * @param {Function} callback The completion handler called after the data has updated
 */
DbOperations.prototype.updateMetadaStatus = function updateMetadaStatus (fileId, callback) {
  // TODO: Need to finalize 'status' names
  const updateQuery = squelPostgres.update()
  .table('files')
  .set('status', 'finished')
  .where('id = ?', fileId)
  .toParam();
  executeQuery(this.pool, updateQuery, callback);
};

/**
 * Reads file's metadata
 * @param {Object} filters to be applied on file's table
 * @param {Function} callback The completion handler called after the data has been read
 */
DbOperations.prototype.readMetadata = function readMetadata (filters, callback) {
  const exp = squelPostgres.expr()
        .and(
          squelPostgres.expr()
            .or('file_date >= ?', filters.startDate || null)
            .or('$1 is null')
        )
        .and(
          squelPostgres.expr()
            .or('file_date <= ?', filters.endDate || null)
            .or('$2 is null')
        )
        .and(
          squelPostgres.expr()
            .or('file_type_hierarchy && ?', filters.fileHierarchy || null)
            .or('$3 is null')
        )
        .and(
          squelPostgres.expr()
            .or('generated_name like ?', filters.fileName || null)
            .or('$4 is null')
        );

  // TODO: Check if ID is valid
  if (filters.id) {
    exp.and('id = ?', filters.id);
  }

  // TODO: add filter on other fields.
  const selectMetadata = squelPostgres.select()
  .from('files')
  .where(exp)
  .order('file_date')
  .toParam();
  executeQuery(this.pool, selectMetadata,
    (err, result) => {
      if (err) {
        callback(err);
        return;
      }
      const records = [];
      result.rows.forEach((item) => {
        records.push({
          fileId: item.id,
          fileDate: item.file_date,
          recordDate: item.record_date,
          originalName: item.original_name,
          generatedName: item.generated_name,
          mimeType: item.mime_type,
          fileExtension: item.file_extension,
          fileSource: item.source,
          fileChecksum: item.file_sha256sum,
          fileChunkSize: item.chunk_size,
          fileHierarchy: item.file_type_hierarchy,
          fileSize: item.file_size,
          attributes: item.attributes,
          fileStatus: item.status,
          numberOfChunks: Math.ceil(item.file_size / item.chunk_size),
        });
      });
      callback(null, records);
    });
};

/**
 * Reads file's data
 * @param {string} fileId Id of the file for which chunks are required
 * @param {Object} chunkNumber sequnce number of the chunk
 * @param {Function} callback The completion handler called after the data has been read
 */
DbOperations.prototype.readChunks = function readChunks (filters, callback) {
  const selectMetadata = squelPostgres.select()
  .from('file_chunks')
  .where('file_id = ?', filters.fileId)
  .where('chunk_number = ?', filters.chunkNumber)
  .toString();
  executeQuery(this.pool, selectMetadata,
    (err, result) => {
      if (err) {
        callback(err);
        return;
      }
      const records = [];
      result.rows.forEach((item) => {
        records.push({
          chunkId: item.id,
          fileId: item.file_id,
          chunkNumber: item.chunk_number,
          chunkChecksum: item.chunk_sha256sum,
          data: item.data,
        });
      });
      callback(null, records);
    });
};

module.exports = {
  DbOperations,
};
