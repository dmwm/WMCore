function(doc) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele) {
        if (ele["Jobs"] === null) {
            emit([ele["Status"], ele["Priority"]], 0);
        } else {
            emit([ele["Status"], ele["Priority"]], ele["Jobs"]);
        }
    }
}
