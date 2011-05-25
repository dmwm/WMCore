function(doc, req) {
	var mainDoc = this
	provides("html", function() {
		var Mustache = require("lib/mustache");
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
			// mark if not run recently
			if ((now - timestamp) < 3600000) {
				task['uptodate'] = true
			}
			data.tasks.push(doc.tasks[name]);
		}
		html_out = Mustache.to_html(mainDoc.templates.TaskStatus, data);
		return {body : html_out,
			headers: {
			"Content-Type": "text/html",
			"Cache-Control" : "no-cache" // used in monitoring so don't cache.
		}};
	})

	provides("json", function() {
		send(toJSON(doc.tasks))
	})
} // end function
