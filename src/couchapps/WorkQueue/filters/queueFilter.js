function(doc, req) {
    if (doc.type && doc.type === "WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"){
        var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
        return (ele['ChildQueueUrl'] === req.query.childUrl && ele['ParentQueueUrl'] === req.query.parentUrl);
    }
    if (doc._deleted){
        // TEMPORARY: Don't replicate specs
        // Need to clean cmsweb databases properly
        if (doc._attachments){
            return false;
        }
        return true;
    }

    return false
}