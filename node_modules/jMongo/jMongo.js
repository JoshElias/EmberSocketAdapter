/*
 * DATABASE MODULE
 */

var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var ObjectID = mongodb.ObjectID;
var async = require('async');
var mutil = require("util");




var defaultOptions = {
	host : "localhost",
	port : 27017,
	name : "test"
}

var JMongo = function( options ) {

	// Set mongo options
	this.host = (typeof(options) == "undefined" || typeof(options.host) == "undefined") ? defaultOptions.host : options.host;
	this.port = (typeof(options) == "undefined" || typeof(options.port) == "undefined") ? defaultOptions.port : options.port;
	this.name = (typeof(options) == "undefined" || typeof(options.name) == "undefined") ? defaultOptions.name : options.name;

	// Set other class properties
	this.defaultConnectionString = "mongodb://"+this.host+":"+this.port+"/"+this.name;
	this.connectionString = this.defaultConnectionString;
	this.connection;
}

// METHODS

// Sets new connectionString and returns the new connection
//
// connectionString: string used to connect you to the database (format: mongodb://0.0.0.0:9999/dbName)
// connectionCallback: function(err, db) 
JMongo.prototype.setConnection = function(connectionString, connectionCallback) {
	try {
	
		// If connectionString isn't given, return with error
		if(typeof(connectionString) == "undefined") {
			connectionCallback( new Error('No connection string was given') )
			return;
		}
	
		async.series([
		
		     // If previously connected, disconnect first
		     function(callback) {
		    	 if( typeof(this.connection) != "undefined" ) {
		    		 this.connection.close( function(err) {
		    			 if(err) callback(err);
		    			 else callback(undefined);
		    		 });
		    	 } else {
		    		 callback(undefined);
		    	 };
		     },
		     
		     // Connect with new connection string
		     function(callback) {
		    	 MongoClient.connect(connectionString, function(err, db) {
		    		 if(err) callback(err);
		    		 else callback(undefined, db);
		    	 });
		     }
		              
		 ], function(err, results) {
			if(err) connectionCallback( err );
			else {
				// Get the DB from the results array
				var db = results[0];
				
				// Set Database variables to reflect new change in connection
				this.connectionString = connectionString;
				this.connection = db;
				
				// Return new connection
				connectionCallback(undefined, db);
			};	
		});
	
	// If there was an error getting the connection, return undefined	
	} catch(err) {
		this.connection = undefined;
		connectionCallback( err );
	};
};

// Retrieve the active connection, returns default if not connected
//
// callback: function(err, db)
JMongo.prototype.getConnection = function(callback) {
	try {  
		
		// If we are already connected, return that connection
		if(this.connection) {
			callback(undefined, this.connection);
			return;
		}
	
		// If not, get a connection
		MongoClient.connect(this.connectionString, function(err, db) {
			if(err) callback( err );
			else {
				this.connection = db;
				callback(undefined, db);
			}
		});
			

	// If there was an error getting the connection, return undefined
	} catch(err) {
		callback( err );
	};
};

// Closes the current mongodb Connection
//
// callback: function(err)
JMongo.prototype.closeConnection = function(callback) {
	try{
		// If our connection isn't valid to begin with, no worries
		if( typeof(this.connection) == "undefined" ) {
			callback(undefined);
			return;
		}
	
		this.connection.close( function(err) {
			if(err) callback( err );
			else callback(undefined);
		});
		
	} catch(err) {
		callback( err );
	};	
};

// Returns collection object by name that you can run queries on
//
// collectionName: name of the collection you wish to get
// callback: function(err, collection)
JMongo.prototype.getCollection = function(collectionName, callback) {
	try {
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else callback(undefined, db.collection(collectionName));
		});
	} catch(err) {
		callback( err );
	};
};


// QUERY FUNCTIONS



// Read Operations: http://docs.mongodb.org/manual/core/read-operations/
// Specifies what data to return when querying the database
//
// skip: amount of matching documents to skip before starting to return them
//        http://docs.mongodb.org/manual/reference/method/cursor.skip/
// limit: only return this amount of documents
//         http://docs.mongodb.org/manual/reference/method/cursor.limit/
// sort: sorts the results of a query 
//		  http://docs.mongodb.org/manual/reference/method/cursor.sort/
JMongo.prototype.readOptions = function(sort, limit, skip) {
	return { 
		sort : (sort) ? sort : {},
	    limit : (limit) ? limit : 0,
	    skip : (skip) ? skip : 0		
	};
};


// FIND


// Finds and returns all documents that match the passed query
// Documentation: http://docs.mongodb.org/manual/reference/method/db.collection.find/
//
// collection: name of the collection you are querying
// query:  formatted string for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// shortList: the properties of the document that you want returned. Instead of returning the entire object.
// callback: function(err, results)
// err: String error returned from mongodb API
// results: array of documents returned that match query
// readOptions: ReadOptions object for specifying what data gets returned
//			     database.readOptions(skip, limit, sort)	
JMongo.prototype.find = function(collection, query, shortList, callback, readOptions ) {
	try {
		// Use appropriate find function for options
		var findFunc = (!readOptions) ? findSimple : findAdvanced;
		query = insertObjectIDs(query);
		findFunc(collection, query, shortList, this, function(err, results) {
			if(err) callback(err);
			else callback(undefined, results);
		}, readOptions);
			
	} catch(err) {
		console.log("Find Err");
		callback( err );
	};
};


// Finds and returns all documents that match the passed query
// Documentation: http://docs.mongodb.org/manual/reference/method/db.collection.find/
//
// collection: name of the collection you are querying
// query:  formatted string for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// shortList: the properties of the document that you want returned. Instead of returning the entire object.
// callback: function(err, results)
// err: String error returned from mongodb API
// results: array of documents returned that match query
var findSimple = function(collection, query, shortList, jMongo, callback ) {
	try {
		jMongo.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				db.collection(collection).find(query, shortList, {}).toArray( function(err, results) {
					if(err) callback( err );
					else callback(undefined, results);
				});
			};
		});
	} catch(err) {
		console.log("Find simple Err");
		console.log(err.stack)
		callback( err );
	};
};

// Finds and returns all documents that match the passed query
// Documentation: http://docs.mongodb.org/manual/reference/method/db.collection.find/
//
// collection: name of the collection you are querying
// query:  formatted string for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// shortList: the properties of the document that you want returned. Instead of returning the entire object.
// callback: function(err, results)
// err: String error returned from mongodb API
// results: array of documents returned that match query
// readOptions: ReadOptions object for specifying what data gets returned
var findAdvanced = function(collection, query, shortList, jMongo, callback, readOptions) {
	try {
		jMongo.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				db.collection(collection).find(query, shortList, {})
										.sort(readOptions.sort)
									  	.limit(readOptions.limit)
									  	.skip(readOptions.skip)
									  	.toArray( function(err, results) {
					if(err) callback(  err );
					else callback(undefined, results);
				});
			};
		});
	} catch(err) {
		callback( err );
	};
};

// Finds and returns the first document to match the passed query
// Documentation: http://docs.mongodb.org/manual/reference/method/db.collection.findOne/
// collection: name of the collection you are querying
// query:  formatted string for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// shortList: the properties of the document that you want returned. Instead of returning the entire object.
// callback: function(err, results)
// err: String error returned from mongodb API
// result: the documents returned that match query
JMongo.prototype.findOne = function(collection, query, shortList, callback ) {
	try {
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				query = insertObjectIDs(query);
				db.collection(collection).findOne(query, shortList, function(err, result) {
					console.log("result: ",result);
					if(err) callback( err );
					else callback(undefined, result);
				});
			};
		});
	} catch(err) {
		console.log("FindOne Err");
		console.log(err.stack)
		callback( err );
	};
};

JMongo.prototype.findOneByHID = function(collection, hid, callback ) {
	try { 
		// Do not return the following fields
		// Set Current Revision
		var shortList = {
				currentRevision: 0,
				created: 0,
				createdBy: 0,
				revisionHistory: 0,
		};
		
		this.getCollection(collection).findOne( {hid:hid}, shortList, function(err, result) {
			if(err) callback(err);
			else callback(undefined, result);
		});
	} catch(err) {
		callback( err );
	};
};


JMongo.prototype.findAndModify = function(collection, query, update, callback ) {
	try {
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				query = insertObjectIDs(query);
				db.collection(collection).findAndModify(query, {}, update, {}, function(err, object) {
					if(err) callback( err );
					else callback(undefined, object);
				});
			};
		});
	} catch(err) {
		callback( err );
	};
};

// Returns amount of objects that match the passed query
// collection: name of the collection you are querying
// query:  formatted string for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// callback: function(err, result)
// err: String error returned from mongodb API 
// result: amount of objects that match the query *Could be zero*
JMongo.prototype.count = function( collection, query, callback ) {
	try { 
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				query = insertObjectIDs(query);
				db.collection(collection).find(query).count( function(err, result) {
					if(err) callback( err );
					else callback(undefined, result);
				});
			};
		});
	} catch(err) {
		callback(  err );
	};
};

// Checks if a document exists that matches the passed query
// collection: name of the collection you are querying
// query:  formatted string for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// callback: function(err, result)
// err: String error returned from mongodb API
// results: true of false if object exists in database
JMongo.prototype.exists = function( collection, query, callback ) {
	try { 
		query = insertObjectIDs(query);
		this.find(collection, query, {_id:1}, function(err, results) {
			if(err) callback(err);
			else callback(undefined, (results.length > 0));
		});
	} catch(err) {
		callback( err );
	};
};

/*
Database.prototype.existsById = function( collection, id, callback ) {
	try { 
		// Make sure the passed ID is a BSON.ObjectID
		var oid = getOID(id);
		if(!oid) throw new Error("BAD ID: "+id);
		
		getCollection(collection).find(query, {_id:oid}, function(err, result) {
			if(err) callback(err);
			else callback(undefined, result);
		});
	} catch(err) {
		callback('Exception Found in Database.existsById: '+err.stack);
	};
};
*/

// UPDATES

// Finds and updates parameter(s) of all documents matching the passed query
// Documentation: http://docs.mongodb.org/manual/reference/method/db.collection.update/
// collection: name of the collection you are querying
// query: formatted object for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// change: formatted object for the change you are making to the found document
// 			http://docs.mongodb.org/manual/tutorial/modify-documents/
// callback: function(err, result)
// err: String error returned from mongodb API
// results: documents updated by the query
JMongo.prototype.update = function( collection, query, change, callback ) {
	try { 
		this.getConnection( function(err, db) {
			if(err) callback( err, undefined);
			else {
				query = insertObjectIDs(query);
				db.collection(collection).update(query, change, {multi:true, safe:true}, function(err) {
					if(err) callback( new merror( merror.code.FIND, 'Database failed to run update query: '+query, err) );
					else callback(undefined);
				});
			};
		});
	} catch(err) {
		callback(  err );
	};
};

// Finds and updates parameter(s) of the first document that matches the passed query
// collection: name of the collection you are querying
// query:  formatted object for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// change: formatted object for the change you are making to the found document
//			http://docs.mongodb.org/manual/tutorial/modify-documents/
// callback: function(err, result)
// err: String error returned from mongodb API
// result: document updated by the query
JMongo.prototype.updateOne = function( collection, query, change, callback ) {
	try { 
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				query = insertObjectIDs(query);
				db.collection(collection).update(query, change, {multi:false, safe:true}, function(err) {
					if(err) callback( err );
					else callback(undefined);
				});
			};
		});
	} catch(err) {
		callback( err );
	};
};

// Finds and updates parameter(s) of the first document that matches the passed query
// If no document is found, the document is added
// collection: name of the collection you are querying
// query:  formatted object for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// change: formatted object for the change you are making to the found document
//			http://docs.mongodb.org/manual/tutorial/modify-documents/
// callback: function(err, result)
// err: String error returned from mongodb API
// result: document updated by the query
JMongo.prototype.upsert = function( collection, query, change, callback ) {
	try { 
		this.getConnection( function(err, db) {
			if(err) callback( err );
			else {
				query = insertObjectIDs(query);
				db.collection(collection).update(query, change, {multi:false, upsert:true, safe:true}, function(err) {
					if(err) callback( err );
					else callback(undefined);
				});
			};
		});
	} catch(err) {
		callback( err );
	};
};


/*
Database.prototype.updateOneByID = function( collection, id, change, callback ) {
	try { 
		// Make sure the passed ID is a BSON.ObjectID
		var oid = getOID(id);
		if(!oid) throw new Error("BAD ID: "+id);
		
		getCollection(collection).update( {_id:oid}, change, {multi:false}, function(err) {
			if(err) callback(err);
			else callback(undefined);
		});
	} catch(err) {
		callback('Exception Found in Database.updateOneByID: '+err.stack);
	};
};
*/

// INSERT

// Inserts the given document into the specified collection
// Documentation: http://docs.mongodb.org/manual/reference/method/db.collection.insert/
// collection: name of the collection you are querying
// doc: document you are inserting
// callback: function(err)
// err: String error returned from mongodb API
JMongo.prototype.insert = function( collection, doc, callback ) {
	try { 
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				db.collection(collection).insert(doc, {safe:true}, function(err) {
					if(err) callback( err );
					else callback(undefined);
				});
			};
		});
	} catch(err) {
		callback( err );
	};
};


//

// Removes all documents from the specified collection that match the passed query
// Documentation: http://docs.mongodb.org/manual/reference/method/db.collection.remove/
// collection: name of the collection you are querying
// query:  formatted object for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// callback: function(err)
// err: String error returned from mongodb API
JMongo.prototype.remove = function( collection, query, callback ) {
	try { 
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				query = insertObjectIDs(query);
				db.collection(collection).remove(query, function(err) {
					if(err) callback( err );
					else callback(undefined);
				});
			};
		});
	} catch(err) {
		callback( err );
	};
};

// Removes the first document from the specified collection that matches the passed query
// collection: name of the collection you are querying
// query:  formatted object for mongo query http://docs.mongodb.org/manual/tutorial/query-documents/
// callback: function(err)
// err: String error returned from mongodb API
JMongo.prototype.removeOne = function( collection, query, callback ) {
	try { 
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				query = insertObjectIDs(query);
				db.collection(collection).remove(query, true, function(err) {
					if(err) callback( err );
					else callback(undefined);
				});
			};
		});
	} catch(err) {
		callback( err );
	};
};


// COLLECTIONS

// Removes specified collection from the current database
//
// collection: name of the collection you are querying
// callback: function(err)
JMongo.prototype.removeCollection = function( collection, callback ) {
	try { 
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				db.collection(collection).drop();
				callback(undefined);
			}
		});
	} catch(err) {
		callback( err );
	};
};

// Retrieves the names of all the collections in the current Database
//
// callback: function(err, collectionNames)
JMongo.prototype.getCollectionNames = function (callback) {
	try {
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				db.collectionNames({}, {namesOnly:true}, function(err, collectionNames) {
					if(err) callback( err );
					else callback( undefined, collectionNames);
				});
			}
		});
		
	} catch(err) {
		callback( err );
	}
};

// Checks if the specified collection exists in the current Database
//
// name: name of the collection you are checking the existence of
// callback: function(err, exists)
JMongo.prototype.collectionExists = function(name, callback) {
	try {
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				db.collectionNames(name, {namesOnly:true}, function(err, collectionNames) {
					if(err) callback( err );
					else callback(undefined, (collectionNames.length > 0));
				});
			}
		});
		
	} catch(err) {
		callback( err )
	}
};


// Deletes the database currently in use
JMongo.prototype.dropDatabase = function( callback ) {
	try { 
		this.getConnection( function(err, db) {
			if(err) callback(err);
			else {
				db.dropDatabase( function(err) {
					if(err) callback( err );
					else callback(undefined);
				});
			}
		});
	} catch(err) {
		callback( err );
	};
};



// GridFS

// GridStore Options
//
// content-type: specifies the mime Type of the content
// metadata: key value collection of whatever data you want to attach to your new GridStore
// chunkSize : specify the size of chunks your content will be split into when it gets put into a collection
JMongo.prototype.gridStoreOptions = function(contentType, metadata, chunkSize) {
	return {
		contentType : (contentType) ? contentType : {},
		metadata : (metadata) ? metadata : {},
		chunkSize : (chunkSize) ? chunkSize : {}
	};	
};

// Opens/Creates a GridStore that can be written to
// Documentation: https://github.com/mongodb/node-mongodb-native/blob/master/docs/gridfs.md
// filename: name of the file GridStore that will be created
// mode: indicates the operation, can be one of:
// 		"r" (Read): Looks for the file information in fs.files collection, or creates a new id for this object.
//		"w" (Write): Erases all chunks if the file already exist.
//		"w+" (Append): Finds the last chunk, and keeps writing after it.
// options: gridStoreOptions that you want to use to set up your GridStore
JMongo.prototype.openGridStore = function(filename, mode, options, callback) {
	try {
		this.getConnection( function(err, db) {
			if(err) callback(err, undefined);
			else {
				var gs = new mongodb.GridStore(db, filename, mode, options); 
				gs.open( function(err, openedGS) {
					if(err) callback( err );
					else callback(undefined, openedGS);
				});
			}
		});
	} catch(err) {
		callback( err );
	}
};

// Closes an open GridStore object
//
// gs : open GridStore instance you wish to close
JMongo.prototype.closeGridStore = function(gs, callback) {
	try {
		gs.close( function(err) {
			if(err) callback( err );
			else callback(undefined);
		});
		
	} catch(err) {
		callback( err );
	}
};

// Removes the specified GridStore from the database
//
// name : name of the GridStore document you wish to remove
JMongo.prototype.removeGridStore = function(name, callback) {
	try {
		this.getConnection( function(err, db) {
			if(err) callback(err);
			else {
				mongodb.GridStore.unlink(db, name, function(err) {
					if(err) callback( err );
					else callback(undefined);
				});
			}
		});
	} catch(err) {
		callback( err );
	}
};



// ID

var insertObjectIDs = function( query ) {
	for( var key in query ) {
		if(key == "_id")
			query[key] = ObjectID(query[key]);
	}
	return query;
}

// Converts string id into BSON.ObjectID
JMongo.prototype.getOID = function(id) {
	try {
		var oid = new BSON.ObjectID(id);
		return oid;
	} catch (err) {
		return false;
	};
};


// MAIN EXPORTS
module.exports = JMongo;