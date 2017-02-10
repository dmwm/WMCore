function(doc) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    //Not sure why without [] on the key give the invalid json error in rewrite
    if (ele) {
        var jobs = ele['Jobs']
        if (ele["Jobs"] === null)
            jobs = 0
        emit([ele.RequestName], {RequestName: ele.RequestName,
                                 Jobs: jobs,
                                 Team: ele.TeamName,
                                 CompleteJobs: (ele.PercentComplete * jobs / 100)});
    }
}