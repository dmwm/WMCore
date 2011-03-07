// update arbitrary fields
// adapted from http://wiki.apache.org/couchdb/Document_Update_Handlers
function(doc, req) {
	if (!req.query.updates) {
		return [doc, '"No updates provided"'];
	}

	try {
		var updates = JSON.parse(req.query.updates);
	} catch (ex) {
		return [doc, '"Error parsing JSON"'];
	}

	for (var field in updates) {
		var value = updates[field];
		var ele = doc['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'];

		// attempt to preserve type
		var type = typeof(ele[field]);
		if (type === "number") {
			value = parseFloat(value);
		}
		ele[field] = value;
	}

	//record update time
	doc.updatetime = new Date().getTime() / 1000; // epoch seconds

	return [doc, ''];
}