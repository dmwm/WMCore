// atomfeed.js
//
// creates an Atom feed list of alerts


var atomFeed = 
{
	mainUrl: null, // full URL to the couchapp
	
	// define and update API is required by index.html caller's implementation
	// does not make sense for this use case, code here will be used/called
	// differently ...
	
	define: function(input)
	{
	}, // define()
	
	
	update: function()
	{
		console.log("worked");
		
	} // update()
	
	
	/*
	 * Taken from previous implementation.
	 * Stored for reference, not used for the moment.
	 * Not sure how to best and cleanly import and initialize
	 * everything that atom.js needs ... 
	 * 
	 * 
	var path = require("vendor/couchapp/lib/path").init(req);
	var Atom = require("vendor/couchapp/lib/atom");

	// to limit number of alerts per page: limit:10
	var indexPath = path.list('index','index',{descending:true});
	var feedPath = path.list('index','index',{descending:true, format:"atom"});

	// Atom feed
	// if the client requests an atom feed and not html, 
  	// we run this function to generate the feed.
  	provides("atom", function() 
  	{
  		var path = require("vendor/couchapp/lib/path").init(req);
    	// var textile = require("vendor/textile/textile"); n/a in couchapp 0.7.1
    	
    	// we load the first row to find the most recent change date (descending was
    	// set true)
    	var row = getRow();
    	
    	// generate the feed header
    	var feedHeader = Atom.header(
    	{
    		// multiplied by 1000 so that the argument is in milliseconds, not seconds
      		updated: new Date(row.key * 1000), // alert's Timestamp
      		title: "Atom feed: " + designDoc.couchapp.visualisationTitle,
      		feed_id: path.absolute(indexPath),
      		feed_link: path.absolute(feedPath)
      	});
      	// send the header to the client
      	send(feedHeader);
      	
      	// loop over all rows
      	if (row)
      	{
      		do
      		{
      			var alert = row.value;
      			var content = row.value;      			
      			// multiplied by 1000 so that the argument is in milliseconds, not seconds
      			var timestamp = new Date(row.key * 1000); // alert's Timestamp
      			content["Timestamp"] = timestamp;
      			var contentText = ("Type: " + alert.Type + "\n" +
      			                   "Level: " + alert.Level + "\n" +
      			                   "Source: " + alert.Source + "\n" +
      			                   "Host: " + alert.HostName + "\n");
      			var author = row.value.TeamName ? row.value.TeamName : "n/a";
      			// generate the entry for this row
      			var feedEntry = Atom.entry(
      			{
      				entry_id: path.absolute("/" + encodeURIComponent(req.info.db_name) + "/" + encodeURIComponent(row.id)),
      				title: "Alert from " + row.value.Component,
      				content_type: "text", // default is HTML
      				content: contentText,
      				updated: timestamp,
      				author: author,
      				// TODO
      				// implement alert show function
      				alternate: path.absolute(path.show("alert", row.id, {format: "html"}))
      			});
      			// send the entry to client
        		send(feedEntry);
      		} while (row = getRow());
      	} // if
      	// close the loop after all rows are rendered
      	return "\n</feed>\n"; // why not send like above? - with send the closing tag doens't appear
    	    	  		
  	}); // provides("atom", function()	
	*/
		
} // atomFeed