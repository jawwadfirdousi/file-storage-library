'use strict';
const db = require('./db_operations');
const moment = require('moment');
const chunkSize = 512 * 1024; // (512KB) ------ Get it from config -------

// wrapper over db_operations. Initializes db instance to be used for operations.
// Any further parsing/manipulation of objects is done here.
class DataAccess {
  constructor (connectionOptions) {
    this.db = new db.DbOperations();

    if (connectionOptions) {
      this.setConnection(connectionOptions);
    }
  }

  setConnection (connectionOptions) {
    this.db.setConnection(connectionOptions);
  }

  /**
   * End all DBOperations
   */
  close () {
    this.db.close();
  }

  /**
   * Gets the files Metadata object from DB.
   * @param {Object} filters to apply on files table.
   * @param {Function} callback(err, result) The completion handler called
      after the query is executed
   */
  getFile (filters, callback) {
    this.db.readMetadata(filters,
    (err, result) => {
      callback(err, result);
    });
  }

  /**
   * Gets file chunks from DB.
   * @param {string} fileId Id of the file for which chunks are required.
   * @param {integer} chunkNumber sequnce number of the chunk
   * @param {Function} callback(err, result) The completion handler called
      after the data has been read
   */
  fetchChunkData (fileId, chunkNumber, callback) {
    this.db.readChunks({
      fileId: fileId,
      chunkNumber: chunkNumber,
    }, (readErr, readResult) => {
      if (readErr) {
        callback(readErr);
        return;
      } else if (!readResult || readResult.length === 0) {
        callback(new Error(`no data for , ${fileId}`));
        return;
      }
      // always return the first row
      callback(null, readResult[0].data);
    });
  }

  writeMetadata (file, callback) {
    const metadata = {};

    // Allow external deduplication toggle
    metadata.deduplicate = file.deduplicate;
    metadata.id = file.id;
    metadata.fileDate = moment(file.fileDate).utc().format('YYYY-MM-DD HH:mm:ss');
    metadata.originalName = file.originalName;
    metadata.generatedName = file.generatedName;
    metadata.mimeType = file.mimeType;
    metadata.fileExtension = file.fileExtension;
    metadata.fileSource = file.fileSource;
    metadata.fileHierarchy = file.fileHierarchy;
    metadata.fileSize = file.fileSize;
    metadata.fileChecksum = file.fileChecksum;
    metadata.attributes = file.attributes || {};

    metadata.fileStatus = 'new';
    metadata.fileChunkSize = chunkSize; // Get it from config
    metadata.recordDate = moment.utc().format('YYYY-MM-DD HH:mm:ss.SSS');
    this.db.writeMetadata(metadata, (err, result) => {

      if (err) {
        callback(err);
        return;
      }
      if (result && result.length === 1) {
        metadata.fileId = result[0].id;
        metadata.fileDate = result[0].file_date;
        metadata.recordDate = result[0].record_date;
        metadata.originalName = result[0].original_name;
        metadata.generatedName = result[0].generated_name;
        metadata.mimeType = result[0].mime_type;
        metadata.fileExtension = result[0].file_extension;
        metadata.fileSource = result[0].source;
        metadata.fileChecksum = result[0].file_sha256sum;
        metadata.fileChunkSize = result[0].chunk_size;
        metadata.fileHierarchy = result[0].file_hierarchy;
        metadata.fileSize = result[0].file_size;
        metadata.attributes = result[0].attributes;
        metadata.fileStatus = result[0].status;
      }
      callback(null, metadata);

    });
  }

  writeChunkData (chunkData, callback) {
    this.db.writeChunk(chunkData, callback);
  }

  updateMetadataStatus (fileId, callback) {
    this.db.updateMetadaStatus(fileId, callback);
  }

  updateMetadata (metadata, callback) {
    const parsedMetadata = metadata;
    if (parsedMetadata.fileDate) {
      parsedMetadata.fileDate = moment(parsedMetadata.fileDate).utc().format('YYYY-MM-DD HH:mm:ss');
    }
    if (parsedMetadata.recordDate) {
      parsedMetadata.recordDate = moment(parsedMetadata.recordDate)
      .utc().
      format('YYYY-MM-DD HH:mm:ss');
    }
    this.db.updateMetadata(parsedMetadata, callback);
  }
}


module.exports = new DataAccess();
