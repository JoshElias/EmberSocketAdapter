window.App = Ember.Application.create();

App.ApplicationAdapter = SocketAdapter;


// Router

App.Router.map( function() {
	this.resource("users", { path: "/" });
	this.resource("user", { path: "users/:user_id"})
});


// Routes

App.UsersRoute = Ember.Route.extend({
	model : function() {
		var newRecord = {
			firstname : "Eric",
			lastname : "Elias"
		}
		console.log("Creating Record")
		return this.store.find('user');
	}	
});

// Model

App.User = DS.Model.extend({
	firstname : DS.attr('string'),
	lastname : DS.attr('string'),
});

