function(head, req)
{
	var designDoc = this;
	// var Mustache = require("vendor/couchapp/lib/mustache");
	// var List = require("vendor/couchapp/lib/list");
	var path = require("vendor/couchapp/lib/path").init(req);
	var Atom = require("vendor/couchapp/lib/atom");

	// to limit number of alerts per page: limit:10
	var indexPath = path.list('index','index',{descending:true});
	var feedPath = path.list('index','index',{descending:true, format:"atom"});
  
  	// HTML
	// The provides function serves the format the client requests.
  	// The first matching format is sent, so reordering functions changes 
  	// thier priority. In this case HTML is the preferred format, so it comes first.
	provides("html", function() 
	{	
		start({"headers": {"Content-Type": "text/html"}});
		
		send("<html><head><title>" + designDoc.couchapp.visualisationTitle + "</title>\n");
		send("<link rel=\"stylesheet\" href=" + path.asset() + "/style/main.css\" type=\"text/css\">\n");
		send("</head><body>\n");
		send("<br><br><center>\n");
		send("<h1><a href=" + indexPath + ">" + designDoc.couchapp.appName + "</a></h1>\n");
		send("<div id=\"feeds\"><a href=" + feedPath + " target=\"_blank\">");
		send("<img src=\"" + path.asset() + "/img/feed-icon.png\" alt=\"atom feed\"/>");
		send("</a>\n</div>\n");
		send("</center>\n");
		send("<table border=0 align=center cellspacing=20>\n");
		send("<tr><th>Timestamp</th><th>Component</th><th>Source</th><th>details</th>\n");
		send("</tr>\n");
		while (row = getRow()) 
		{
			// row.key = alert.Timestamp
			// multiplied by 1000 so that the argument is in milliseconds, not seconds
			send("<tr><td>" + new Date(row.key * 1000) + "</td>\n");
			send("<td>" + row.value.Component + "</td>\n");
			send("<td>" + row.value.Source + "</td>\n");
			send("<td><a href=" +
			     path.absolute(path.show("alert", row.id, {format: "html"})) +
			     " target=\"_blank\">html</a>&nbsp;|&nbsp;<a href=" +
			     path.absolute(path.show("alert", row.id)) +
			     " target=\"_blank\">text</a>\n");
			send("</td></tr>\n");
		}
		send("</table\n");
		send("</body></html>");
	}); // provides("html", function()
	
	
	
	
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
	
}; // top level function