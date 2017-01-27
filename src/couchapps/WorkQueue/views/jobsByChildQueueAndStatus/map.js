function(doc) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele) {
        if (ele["Jobs"] === null) {
            emit([ele["ChildQueueUrl"], ele["Status"]], 0);
        } else {
            emit([ele["ChildQueueUrl"], ele["Status"]], ele["Jobs"]);
        }
    }
}
