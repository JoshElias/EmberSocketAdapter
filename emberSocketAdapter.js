var socketIO = require("socket.io");
var jMongo = require("jMongo");
var async = require("async");


// Test DB
var database = new jMongo();

var EmberSocketAdapter = function( server ) {
	var io = socketIO(server);

	io.on("connection", function(socket) {
		console.log("Socket Connected");

		socket.on("find", findHandler(socket));
		socket.on("findAll", findAllHandler(socket));
		socket.on("findQuery", findQueryHandler(socket));
		socket.on("findMany", findManyHandler(socket));
		socket.on("create", createHandler(socket));
		socket.on("delete", deleteHandler(socket));
		socket.on("update", updateHandler(socket));
	});
}

var findHandler = function( socket ) {
	return function( data ) {
		console.log("Calling find")
		database.findOne(data.collection, { "_id" : data.id }, {}, function(err, result) {
			if(err) {
				socket.emit("found", { err: err });
			} else {
				result = mongoToEmberIDs(result);
				console.log("Sending Results to Client");
				console.log(result);
				socket.emit("found", { result: result });
			}
		})
	}
}

var findAllHandler = function( socket ) {
	return function( data ) {
		console.log("Calling findAll")
		database.find(data.collection, {}, {}, function(err, results) {
			if(err) {
				socket.emit("foundAll", { err: err });
			} else {
				var results = mongoToEmberIDs(results);
				socket.emit("foundAll", { results: results });
			}
		})
	}
}

var findQueryHandler = function( socket ) {
	return function( data ) {
		var query = emberToMongoIDs(data.query);
		database.find(data.collection, data.query, {}, function(err, results) {
			if(err) {
				socket.emit("foundQuery", { err: err });
			} else {
				var results = mongoToEmberIDs(results);
				socket.emit("foundQuery", { results: results});
			}
		})
	}
}

var findManyHandler = function( socket ) {
	return function( data ) {
		console.log("Calling find many")
		var findFuncs = [];
		var getFindFunc = function(key) {
			return function( callback ) {
				database.findOne(data.collection, {"_id":data.ids[key]}, {}, function(err, result) {
					if( err ) callback( err );
					else callback( undefined, result );
				});
			}
		}
		for( var key in data.ids) {
			findFuncs.push( getFindFunc(key) );
		}

		async.series( findFuncs, function( err, results ) {
			if(err) socket.emit("foundMany", { err: err });
			else {
				var results = mongoToEmberIDs(results);
				socket.emit("foundMany", { results: results});
			}
		});
	}
}

var createHandler = function( socket ) {
	return function( data ) {
		console.log("Calling Create")
		database.insert(data.collection, data.record, function(err) {
			if(err) {
				socket.emit("created", { err: err });
			} else {
				socket.emit("created", {} );
			}
		})
	}
}

var deleteHandler = function( socket ) {
	return function( data ) {
		console.log("Calling Delete")
		var record = emberToMongoIDs( data.record );
		database.removeOne(data.collection, record, function(err) {
			if(err) {
				socket.emit("deleted", { err: err });
			} else {
				socket.emit("deleted", {} );
			}
		})
	}
}

var updateHandler = function( socket ) {
	return function( data ) {
		console.log("Calling Update")
		
		var id = data.record["id"];
		delete data.record["id"];

		database.updateOne(data.collection, {"_id":id}, data.record, function(err) {
			if(err) {
				socket.emit("updated", { err: err });
			} else {
				socket.emit("updated", {} );
			}
		})
	}
}



var convertIDs = function( oldKey, newKey, results ) {
	
	if(Array.isArray(results)) {
		for( var i = 0; i < results.length; i++ ) {
			var result = results[i];
			result[newKey] = result[oldKey];
			delete result[oldKey]
		}
	} else {
		for( var key in results ) {
			if( key == oldKey ) {
				results[newKey] = results[oldKey];
				delete results[oldKey];
			}
		}
	}
	return results;
}

var mongoToEmberIDs = function( results ) {
	return convertIDs("_id", "id", results);
}

var emberToMongoIDs = function( results ) {
	return convertIDs("id", "_id", results);
}


// MAIN EXPORTS
module.exports = EmberSocketAdapter;


