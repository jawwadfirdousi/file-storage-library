'use strict';
const dataAccess = require('./data_access');
const File = require('./file');
const moment = require('moment');


// Main interface for file storage and retrieval

/**
 * Gets list of parsed file metadata that can stream out their data.
 * @param {Object} filters to be applied on file's table
 * @param {Function} callback The completion handler called after the data has been read
 */
function getFiles (filter, callback) {

  // format filters to make them usable in SQL query.
  const queryFilter = filter || {};
  // convert to specific format to be used with Postgres, using moment.

  queryFilter.startDate = queryFilter.startDate ?
    moment(queryFilter.startDate).utc().format('YYYY-MM-DD HH:mm:ss') : null;
  queryFilter.endDate = queryFilter.endDate ?
      moment(queryFilter.endDate).utc().format('YYYY-MM-DD HH:mm:ss') : null;
  queryFilter.fileHierarchy = queryFilter.fileHierarchy ? `{ ${queryFilter.fileHierarchy} }` : null;
  queryFilter.fileName = queryFilter.fileName ? `%${queryFilter.fileName}%` : null;

  dataAccess.getFile(queryFilter, (err, records) => {
    if (err) {
      callback(err);
      return;
    } else if (!records) { // validation check. Return empty array, if no record
      callback(null, []);
      return;
    }
    const allFiles = [];
    for (let i = 0; i < records.length; i++) {
      allFiles.push(new File(records[i]));
    }
    callback(err, allFiles);
  });
}

function saveFile (file, callback) {
  dataAccess.writeMetadata(file, (err, metadata) => {
    if (err) {
      callback(err);
      return;
    }
    const fileObj = new File(metadata);
    callback(null, fileObj);
  });
}

function updateFile (file, callback) {
  dataAccess.updateMetadata(file, callback);
}

module.exports = {
  getFiles,
  saveFile,
  updateFile,
  close: () => { dataAccess.close(); },
};
