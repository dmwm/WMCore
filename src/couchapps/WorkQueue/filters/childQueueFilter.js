function(doc, req) {
    if (doc._deleted){
       return false;
    }
    
	if (doc.type && doc.type === "WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement") {
		var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
		if (ele['ChildQueueUrl'] === req.query.queueUrl) {
			return true;
		}
	}
	return false;
}