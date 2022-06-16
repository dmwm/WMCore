function(doc, req) {
    if (doc._deleted){
       return false;
    }
    
    if (doc.type && doc.type === "WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"){
        var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
        return (ele['ChildQueueUrl'] === req.query.childUrl && ele['ParentQueueUrl'] === req.query.parentUrl);
    }
    return false;
}
