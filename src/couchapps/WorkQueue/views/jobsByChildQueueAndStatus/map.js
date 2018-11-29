function(doc) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele) {
        emit([ele["ChildQueueUrl"], ele["Status"]], ele["Jobs"]);
    }
}
