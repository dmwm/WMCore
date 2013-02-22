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

    try {
        var options = JSON.parse(req.query.options);
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
		// Check if we are doing incremental updates
		// Currently only supports arrays
        if ("incremental" in options && options["incremental"]){
            if ((Object.prototype.toString.call(ele[field]) === '[object Array]')){
                for(var i = 0; i < value.length; i++){
                    singleValue = value[i]
                    ele[field].push(singleValue)
                }
            } else {
                // Unsupported type
                ele[field] = value;
            }
        } else {
            ele[field] = value;
        }
	}

	//record update time
	doc.updatetime = new Date().getTime() / 1000; // epoch seconds

	return [doc, ''];
}
