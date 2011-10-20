function(doc, req) 
{
	var designDoc = this;
	var path = require("vendor/couchapp/lib/path").init(req);
	require("vendor/json2html/json-min");
	var json2html = require("vendor/json2html/parse");

	getHtml = function(json)
	{
		// start({"headers": {"Content-Type": "text/html"}}); // seems to throw error
		var r = "";
		r = "<html><head><title>" + designDoc.couchapp.visualisationTitle + "</title>\n";
		r += "<link rel=\"stylesheet\" href=\"" + path.asset() + "/style/json2html-style.css\" type=\"text/css\">\n";
		r += "</head><body><br><center>\n";
		var s = JSON.stringify(json);
		r += json2html.parse(s);
		r += "</center></body></html>\n";
		return r;
	}
	
	var c = doc;
	// remove some entries unnecessary for html/text output
	delete c["_rev"];
	delete c["_id"];
	delete c["_revisions"];
	// multiplied by 1000 so that the argument is in milliseconds, not seconds
	if (doc.Timestamp != null)
	{
		c["Timestamp"] = new Date(doc.Timestamp * 1000);
	}

	// branch according to requested output format	
	if (req.query.format == "html")
	{
		return getHtml(c);
	}
	
	// by default return text
	return JSON.stringify(c);	
}