'use strict';

const minimist = require('minimist');
const path = require('path');
const fs = require('fs');
const async = require('async');
const assert = require('assert');
const crypto = require('crypto');
const _ = require('lodash');
const moment = require('moment');

const fileAccess = require('./lib/file_access');
// Data access wrapper
const dataAccess = require('./lib/data_access');


function formatFileSize (bytes) {
  if (bytes < 1024) {
    return { size: bytes, unit: 'B' };
  }
  const multiplier = 1024;
  const precision = Math.pow(10, 2);
  const power = Math.floor(Math.log(bytes) / Math.log(multiplier));
  const size = Math.floor((bytes * precision) / Math.pow(multiplier, power)) / precision;
  const units = ['B', 'kB', 'MB', 'GB', 'TB', 'EB'];
  const unit = units[power];
  return {
    size,
    unit,
  };
}

function getHash (data) {
  const hasher = crypto.createHash('sha256');
  hasher.update(data);
  return hasher.digest('hex');
}

/**
* makes sure the folders/sub-folders are created, works recursivley.
 * @param {string} dirPath (complete physical path to create folder)
 * @param {function} callback (call without parameters when job is completed,
      otherwise with error as argument)
 */
function ensureFolderExists (dirPath, callback) {
  fs.mkdir(dirPath, (err) => {
    if (err && err.code === 'EEXIST') {
      return callback(null);
    }

    const current = path.resolve(dirPath);
    const parent = path.dirname(current);

    ensureFolderExists(parent, (privateEnsureErr) => {
      if (privateEnsureErr) {
        return callback(err);
      }

      fs.mkdir(current, (mkdirErr) => {
        if (mkdirErr && mkdirErr.code === 'EEXIST') {
          return callback(null);
        }
        return callback();
      });
      return null;
    });
    return null;
  });
}

if (require.main === module) {

  // On-demand config loading for direct exection
  /* eslint-disable global-require */
  const config = require('./config.json');
  /* eslint-enable global-require */

  const args = minimist(process.argv.slice(2));
  const filter = args;
  const destDir = args.path;

  dataAccess.setConnection(config.postgresUrl);

  fileAccess.getFiles(
    filter,
  (err, files) => {
    if (err) {
      console.log(err);
      return;
    }

    // for lodash map.
    function mapZipObject (currentItem) {
      return _.zipObject(['fileHierarchy', 'metadata'], currentItem);
    }

    // group files based on the type/hierarchy
    const grouped = _.chain(files)
    .groupBy('metadata.fileHierarchy')
    .toPairs()
    .map(mapZipObject)
    .value();

    const totalBytes = _.sumBy(files, 'metadata.fileSize');
    const formattedSize = formatFileSize(totalBytes);

    if (!destDir) {
      _(grouped).forEach((value) => {
        console.log(`${value.metadata.length} file(s) of type ${value.fileHierarchy} `);
      });
      console.log(`Total file count: ${files.length}`);
      console.log(`Total file size: ${formattedSize.size} ${formattedSize.unit}`);

      // Clean up after all operations have finished
      fileAccess.close();
      return;
    }

    // Total number of digits required to display the file count
    const counterDigits = Math.ceil(Math.log10(files.length));

    let downloadCounter = 1;
    async.eachSeries(files, (file, next) => {

      const subPath = path.join.apply(path, file.metadata.fileHierarchy);
      let destPath = path.join(destDir, subPath);

      // If --flat argument is provided then do not make sub-folders/hierarchy.
      if (args.flat) {
        destPath = destDir;
      }

      ensureFolderExists(destPath, (dirErr) => {
        if (dirErr) {
          console.log('Folder does not exist, neither could create it.');
          return next(dirErr);
        }

        // Full path including file name
        const filePath = path.join(destPath, file.metadata.generatedName);

        file.getReadStream(readStream => {
          const writeStream = fs.createWriteStream(filePath);

          readStream.pipe(writeStream);
          const chunkList = [];

          readStream.on('data', (chunk) => {
            chunkList.push(chunk);
          });

          readStream.once('end', () => {
            const buffer = Buffer.concat(chunkList);
            const paddedCounter = _.padStart(downloadCounter, counterDigits, ' ');
            console.log(`downloading ${paddedCounter} / ${files.length} ` +
              `${moment(file.metadata.fileDate).format('YYYY-MM-DD')} ` +
              `${file.metadata.fileHierarchy}`);
            downloadCounter++;
            assert(file.metadata.fileChecksum === getHash(buffer), `something is
            wrong with ${file.metadata.generatedName}`);
            next(null);
          });

          readStream.once('error', (readErr) => {
            console.log(`Error while saving ${filePath}: ${readErr}`);
            next(err);
          });
        });
        return null;
      });
    }, (seriesErr) => {
      if (seriesErr) {
        console.log(seriesErr);
        return;
      }
      _(grouped).forEach((value) => {
        console.log(`${value.metadata.length} file(s) of ${value.fileHierarchy} `);
      });
      console.log(`Total file count: ${files.length}`);
      console.log(`Total file size: ${formattedSize.size} ${formattedSize.unit}`);

      // Clean up after all operations have finished
      fileAccess.close();
    });
  });
}

module.exports = Object.assign(
  {
    // Helper functions
    setConnection: (...args) => { dataAccess.setConnection(...args); },

    // Full module export
    dataAccess,
    fileAccess,
  },

  // Library methods
  fileAccess
);
