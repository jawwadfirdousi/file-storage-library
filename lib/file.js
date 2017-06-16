'use strict';

const chunkStream = require('./file_chunk_streaming');
const stream = require('stream');
// File Schema. Contains streaming interface to stream data.

class File {
  constructor (obj) {
    this.metadata = obj;
  }

  /**
   * Collects all chunks for the file in an array.
   * @param {Function} callback(err, result[]) The completion handler called after the chunks
      has been collected in an array.
   */
  getAllChunks (callback) {
    this.getReadStream((readableStream) => {
      const allChunks = [];
      readableStream.on('data', (chunk) => {
        allChunks.push(chunk);
      });
      readableStream.on('end', () => {
        callback(null, Buffer.concat(allChunks));
      });
      readableStream.on('error', (streamErr) => {
        callback(streamErr, null);
      });
    });
  }

  /**
   * Gives the read stream to get data.
   * @param {Function} callback(ReadableStream) The completion handler with called
      with ReadableStream.
   */
  getReadStream (callback) {
    callback(new chunkStream.ReadChunkStream(this.metadata.fileId, this.metadata.numberOfChunks));
  }

  /**
   * Gives the write stream to write data into DB.
   * @param {Function} callback(WritableStream) The completion handler called
      with WritableStream.
   */
  getWriteStream (callback) {
    // TODO: Get this from parameter
    const maxChunkSize = this.metadata.fileChunkSize;
    const fileId = this.metadata.fileId;

    // return the empty (dummy) stream when status is finished, so that we dont
    // write same data again.
    if (this.metadata.fileStatus === 'finished') {
      const writable = new stream.Transform();
      writable.on('data', (chunk, dataCallback) => {
        dataCallback();
      });
      writable._transform = function transform (chunk, encoding, done) {
        this.emit('data', chunk, done);
      };
      writable._flush = function flush (done) {
        done(null);
      };
      callback(writable);
      return;
    }
    const writableStream = new chunkStream.WriteChunkStream(maxChunkSize, fileId);
    callback(writableStream);
  }
}

module.exports = File;
