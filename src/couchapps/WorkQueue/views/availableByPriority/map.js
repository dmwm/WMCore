function(doc, site) {
	var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
	if (ele && ele["Status"] === "Available") {
		var now = new Date().getTime() / 1000; // epoch seconds
		// TODO: Make priority modifier configurable
		var priority = ele["Priority"] + (0.0125 * (now - doc.timestamp));
		emit(priority, {"_id" : doc["_id"]});
	}
}
