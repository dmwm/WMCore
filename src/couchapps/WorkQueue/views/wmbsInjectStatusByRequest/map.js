function(doc) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele) {
        if (!ele.OpenForNewData &&
            (ele.Status == 'Running' || ele.Status == 'Done'
             || ele.Status == 'Failed')) {
            emit(ele.RequestName, true);
        } else {
            emit(ele.RequestName, false);
        }
    }
}