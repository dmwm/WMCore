function(doc, site) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele && ele["Status"] === "Available") {
        // TODO: Make priority modifier configurable
        // modifier so 1 hour queue time == +1 priority boost
        var priority = ele["Priority"] - (doc.timestamp * (1. / 60 / 60))
        emit(priority, {"_id" : doc["_id"]});
    }
}
