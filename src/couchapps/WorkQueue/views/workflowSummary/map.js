function(doc) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    //Not sure why without [] on the key give the invalid json error in rewrite
    if (ele) {
        emit([ele.RequestName], {RequestName:ele.RequestName, Jobs: ele.Jobs,
                                 Team: ele.TeamName,
                                 CompleteJobs: (ele.PercentComplete * ele.Jobs / 100)});
    }
}