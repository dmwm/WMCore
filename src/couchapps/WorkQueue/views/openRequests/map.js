function(doc) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele && ele["OpenForNewData"] === true) {
            emit(ele["RequestName"], null);
    }
}