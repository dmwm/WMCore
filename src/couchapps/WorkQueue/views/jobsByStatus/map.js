function(doc) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele) {
        emit(ele["Status"], ele["Jobs"]);
    }
}
