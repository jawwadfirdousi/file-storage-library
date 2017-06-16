# files storage service


Description
===========

Service to store/retrieve files to/from database in chunks. This library is ideal to store large files in DB.
This library uses postgers to store binary data. However any database can be used by tweaking some functions in lib/db_operations.js

Meta data of files in stored in table *file* and file data is stored in *file_chunks* table.

Installation
============

All local dependencies can be installed with npm using

	% npm install


Configuring
===========

Create 2 tables in postgres. Create table script is present in **postgres_db_scripts.sql**
Also create an enum *file_status* to track if there was any error while writing chunks

Running
=======

Require the exported library as:

```javascript
storageService = require('file-storage-service');

```

Set postgres connection:

```javascript
storageService.setConnection('tcp://postgres:postgres@127.0.0.1:5432/awattar_files');
```
-----------------
##### Saving Files

Assign metadata interface (information about the file) object:

```javascript
let metadata = {
	fileDate: new Date(),
	originalName: 'originalName',
	generatedName: 'filename', // if multiple files with same name change this
	mimeType: 'mimeType',
	fileExtension: 'extension',
	fileSource: 'source',
	fileHierarchy: 'hierarchy', // type is array, set if file belongs to some category
	fileSize: 1024, // in bytes
	fileChecksum: 'sha256sum', // SHA 256 of the content
	attributes: {}, // type is json, set if there is any optional information
};
```
Save meta data in DB with saveFile function, the callback will return file object (this file object contains saved metadata), then from file object get the writable stream with function getWriteStream. Actual bytes can be saved in DB with writableStream.

```javascript
storageService.saveFile(metadata, (err, file) => {
	if (err) {
		callback(err);
		return;
	}

	// 'finished' status means the metadata is already present in the db with its data.
	// Its good to implement this check to avoid writing the duplicate file.
	if (file.metadata.fileStatus === 'finished') {
		console.log(`${file.metadata.generatedName} File Already present`);
		next(null);
		return;
	}
	file.getWriteStream((writableStream) => {
		writableStream.on('error', (streamErr) => {
			callback(streamErr);
		});
		writableStream.on('end', () => {
			next();
		});
		writableStream.write(buffer); // write data through stream
		writableStream.end();
	});
});
```

---

##### Retrieving files

Assign filters if fitering on records is needed. Leaving filter object empty will return all metadata.

```javascript
const filter = {
	queryFilter.startDate: new Date(),
  queryFilter.endDate: new Date(),
  queryFilter.fileHierarchy = 'heirarchy1, heirarchy2,....,...'; // comma separated values
  queryFilter.fileName = 'accounts_file.xls';
}
storageService.getFiles (filter, (err, files) => {
	if (err) {
		throw err,
	}
	// files is an array of metadata
	console.log(files.length);

	for (var i = 0; i < files.length; i++) {
		files[i].getReadStream(readStream => {

			readStream.on('data', (chunk) => {

			});

			readStream.once('end', () => {

			});

			readStream.once('error', (readErr) => 	{

			});
		})	;
	}
});
```

If directly running this library via index.js following arguments can be applied. All arguments are Optional and order is not important.

- **--path** [string without quotes] physical path where you want the files to be stored.
- **--startDate** [YYYY-MM-DD] e.g 2016-01-01
- **--endDate** [YYYY-MM-DD] e.g 2016-12-31
- **--fileHierarchy** [string without quotes (comma separated without space, if multiple values)] e.g ess,mscons
- **--fileName** [string without quotes (single value only)] e.g DATA_XY112233
- **--flat** [no value required] if this parameter is provided then files will be saved in a single folder, without hierarchy.
