var express = require("express")
var app = express();
var server = require("http").Server(app);
var path = require("path");
var util = require("util")
var emberSocketAdapter = require("./emberSocketAdapter")(server);



// Set up Express Web Server
app.set('port', 3000);
app.set('views', __dirname+path.sep+'views');
app.use(express.static(__dirname+path.sep+'public'));


// Routes
app.get("/", function( req, res ) {
	res.sendfile(__dirname+path.sep+"views"+path.sep+"index.html");
});


server.listen(app.get('port'), function() {
	console.log("Server listening on port: ", app.get('port'));
})

/*
var jMongo = require("jMongo");
var database = new jMongo();

database.find( "user", {}, {}, function( err, results ) {
	console.log(err);
	console.log(results);
	database.find( "user", {"_id":"53a478493d09aa3e954493d6"}, {}, function( err, results ) {
		console.log(err);
		console.log(results);
	})
})
*/