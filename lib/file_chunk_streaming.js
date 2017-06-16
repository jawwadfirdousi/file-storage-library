'use strict';

const dataAccess = require('./data_access');
const stream = require('stream');
const crypto = require('crypto');
const async = require('async');


function getHash (data) {
  const hasher = crypto.createHash('sha256');
  hasher.update(data);
  return hasher.digest('hex');
}

function getChunkObject(fileId, chunkNumber, data) {
  const returnObj = {
    fileId: fileId,
    chunkChecksum: getHash(data),
    chunkNumber: chunkNumber,
    data: data,
  };
  return returnObj;
}

// Read stream for getting file chunks from db as stream.
class ReadChunkStream extends stream.Readable {
  constructor (fileId, chunkCount, options) {
    super(options);
    // TODO: use starting index 0
    this.index = 0;
    this.fileId = fileId;
    this.chunkCount = chunkCount;
  }

  // overridden method
  _read () {
    const i = this.index++;
    if (i >= this.chunkCount) {
      // Push null to let the stream know there is no more data.
      this.push(null);
      return;
    }
    dataAccess.fetchChunkData(this.fileId, i, (err, chunk) => {
      if (err) {
        // Emit error if theres any error is returned from db or while parsing the record.
        this.emit('error', err);
        return;
      }
      this.push(chunk);
    });
  }
}


class WriteChunkStream extends stream.Transform {
  constructor (maxChunkSize, fileId) {
    super();
    this.chunkLimit = maxChunkSize;
    this.fileId = fileId;
    this.error = null;
    // a leftover/carry-over buffer after breaking into pieces w.r.t maxChunkSize
    // also used if the complete data is smaller than the maxChunkSize
    this.remainderBuffer = new Buffer('');
    // used to track/measure sequence (numbering) of the chunks.
    this.chunkCounter = 0;

    this.on('data', (chunk, callback) => {
      // current buffer (remainder + new chunk) to be used for size comparision.
      const currentBuffer = Buffer.concat([this.remainderBuffer, chunk]);
      // Size of current buffer
      const currentBufferSize = currentBuffer.length;

      // Case 1: add currentBuffer to remainder buffer because it is smaller than maxChunkSize.
      // It will either be concatenated with the next chunk or will be written to DB on finish event
      if (currentBufferSize < maxChunkSize) {
        this.remainderBuffer = currentBuffer;
        callback(null);
        return;
      }

      // Case 2: The size of current buffer is >= maxChunkSize break the chunk into piece.
      const sliceCount = Math.ceil(currentBufferSize / maxChunkSize); // number of pieces to make

      // empty the remainder buffer, if mod is 0 (means chunk is broken into exaclty
      // equal size piece(s))
      if (currentBufferSize % maxChunkSize === 0) {
        this.remainderBuffer = new Buffer('');
      }
      async.timesSeries(sliceCount, (i, next) => {
        const startOffset = i * maxChunkSize; // will always start with 0 index
        const endOffset = (maxChunkSize * (i + 1));
        // sliced chunk to write in db
        const tempChunk = currentBuffer.slice(startOffset, endOffset);

        // when tempChunk length is not same as the maxChunkSize, add it to remainder buffer
        // (this must be the last temp piece).
        if (tempChunk.length !== maxChunkSize) {
          // add the unequal chunk to the remainder buffer.
          this.remainderBuffer = tempChunk;
          // (this must be the last iteration)
          // call next to continue with the execution.
          next(null, tempChunk);
          return;
        }

        // when tempChunk length is same as our maxChunkSize, write in the DB.
        dataAccess.writeChunkData(getChunkObject(this.fileId, this.chunkCounter, tempChunk),
        (err, result) => {
          // call next to continue with the next iteration.
          next(err, result);
        });
        // increment the counter after issuing insert query.
        this.chunkCounter++;
      }, (err) => {
        callback(err);
      });
    });
  }

  _transform (chunk, encoding, callback) {
    this.emit('data', chunk, callback);
  }

  _flush (callback) {

    // If there is no data in remainderBuffer, just update the metadata status to 'finished'
    // and exit.
    if (this.remainderBuffer.length === 0) {
      dataAccess.updateMetadataStatus(this.fileId, (updateErr) => {
        callback(updateErr);
      });
      return;
    }

    // write the remaining chunk and update the metadata status.
    dataAccess.writeChunkData(getChunkObject(this.fileId, this.chunkCounter, this.remainderBuffer),
    (err) => {
      if (err) {
        callback(err);
        return;
      }

      dataAccess.updateMetadataStatus(this.fileId, (updateErr) => {
        callback(updateErr);
      });
    });
    return;
  }
}

module.exports = {
  ReadChunkStream,
  WriteChunkStream,
};
