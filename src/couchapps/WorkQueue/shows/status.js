function(doc, req) {
	var mainDoc = this
	var data = {tasks : []}
	var now = new Date()

	for (var name in doc.tasks) {
		if (doc.tasks.hasOwnProperty(name) === false) {
			continue;
		}
		var task = doc.tasks[name]
		// convert {} to [] - must be a better way...
		task['name'] = name;
		// convert time stamp to Date object
		var timestamp = new Date(task['timestamp'] * 1000);
		task['timestamp'] = timestamp.toUTCString();
		// mark if not run recently (2 hours)
		if ((now - timestamp) < 7200000) {
			task['uptodate'] = true
		}
		data.tasks.push(doc.tasks[name]);
	}

	provides("html", function() {
		var Mustache = require("lib/mustache");
		html_out = Mustache.to_html(mainDoc.templates.TaskStatus, data);
		return {body : html_out,
			headers: {
			"Content-Type": "text/html",
			"Cache-Control" : "no-cache" // need to recompute even if etag the same
		}};
	})

	provides("json", function() {
		return {body : toJSON(data),
			headers: {
			"Content-Type": "application/json",
			"Cache-Control" : "no-cache" // need to recompute even if etag the same
		}};
	})
} // end function
