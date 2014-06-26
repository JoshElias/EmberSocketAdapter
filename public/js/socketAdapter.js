var socket = io.connect("http://localhost");

var SocketAdapter = DS.Adapter.extend({
	find : function( store, type, id ) {
		console.log("Calling find in SocketAdapter");
		
		var collection = getCollectionFromEmberType(type);
		return new Ember.RSVP.Promise( function( resolve, reject ) {
			socket.emit("find", { collection: collection, id: id } );
			socket.on("found", function( data ) {
				if( data.err ) {
					Ember.run(null, reject, data.err);
				} else {
					Ember.run(null, resolve, data.result );
				}
			});
		});
	},
	findAll : function( store, type ) {
		console.log("Calling findAll in SocketAdapter");
		
		var collection = getCollectionFromEmberType(type);
		return new Ember.RSVP.Promise( function( resolve, reject ) {
			socket.emit("findAll", { collection: collection } );
			socket.on("foundAll", function( data ) {
				if( data.err ) {
					Ember.run(null, reject, data.err);
				} else {
					Ember.run( null, resolve, data.results );
				}
			});
		});
	},
	findQuery : function( store, type, query ) {
		console.log("Calling findQuery in SocketAdapter");
		
		var collection = getCollectionFromEmberType(type);

		return new Ember.RSVP.Promise( function( resolve, reject ) {
			socket.emit("findQuery", { collection: collection, query: query } );
			socket.on("foundQuery", function( data ) {
				if( data.err ) {
					Ember.run(null, reject, data.err);
				} else {
					Ember.run( null, resolve, data.results );
				}
			});
		});
	},
	findMany : function( store, type, ids ) {
		console.log("Calling find in SocketAdapter");
		
		var collection = getCollectionFromEmberType(type);
		return new Ember.RSVP.Promise( function( resolve, reject ) {
			socket.emit("findMany", { collection: collection, ids: ids } );
			socket.on("foundMany", function( data ) {
				if( data.err ) {
					Ember.run(null, reject, data.err);
				} else {
					Ember.run(null, resolve, data.results );
				}
			});
		});
	},
	createRecord : function( store, type, record ) {
		console.log("Calling create in SocketAdapter");
		
		var collection = getCollectionFromEmberType(type);
		return new Ember.RSVP.Promise( function( resolve, reject ) {
			socket.emit("create", { collection: collection, record: record } );
			socket.on("created", function( data ) {
				if( data.err ) {
					Ember.run(null, reject, data.err);
				} else {
					Ember.run(null, resolve, undefined );
				}
			});
		});
	},
	deleteRecord : function( store, type, record ) {
		console.log("Calling delete in SocketAdapter");
		
		var collection = getCollectionFromEmberType(type);
		return new Ember.RSVP.Promise( function( resolve, reject ) {
			socket.emit("delete", { collection: collection, record: record } );
			socket.on("deleted", function( data ) {
				if( data.err ) {
					Ember.run(null, reject, data.err);
				} else {
					Ember.run(null, resolve, undefined );
				}
			});
		});
	},
	updateRecord : function( store, type, record ) {
		console.log("Calling update in SocketAdapter");
		
		var collection = getCollectionFromEmberType(type);
		return new Ember.RSVP.Promise( function( resolve, reject ) {
			socket.emit("update", { collection: collection, record: record } );
			socket.on("updated", function( data ) {
				if( data.err ) {
					Ember.run(null, reject, data.err);
				} else {
					Ember.run(null, resolve, undefined );
				}
			});
		});
	}
});

var getCollectionFromEmberType = function( emberType ) {
	var className = emberType.toString();
	var nameArr = className.split(".");
	return nameArr[1].toLowerCase();
}