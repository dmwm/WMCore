function(doc, site) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele && ele["Status"] === "Available") {
        emit(ele["Priority"], {"_id" : doc["_id"]});
    }
}
