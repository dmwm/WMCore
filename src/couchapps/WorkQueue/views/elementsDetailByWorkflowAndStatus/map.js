function(doc) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele) {
	    emit([ele.RequestName, ele.Status], {'Id' : doc['_id'],
	                                         'RequestName':ele.RequestName,
	                                         'Inputs': ele.Inputs,
	                                         'Jobs': ele.Jobs,
	                                         'SiteWhitelist': ele.SiteWhitelist,
	                                         'SiteBlacklist': ele.SiteBlacklist,
	                                         'Dbs': ele.Dbs,
	                                         'Task': ele.Task,
	                                         'Priority': ele.Priority,
	                                         'Status': ele.Status,
	                                         'EventsWritten': ele.EventsWritten,
	                                         'FilesProcessed': ele.FilesProcessed,
	                                         'PercentComplete': ele.PercentComplete,
	                                         'PercentSuccess': ele.PercentSuccess,
	                                         'TeamName': ele.TeamName,
	                                         'ChildQueueUrl': ele.ChildQueueUrl,
	                                         'WMBSUrl': ele.WMBSUrl,
	                                         'ACDC': ele.ACDC,
	                                         'InsertTime': doc.timestamp,
	                                         'UpdateTime': doc.updatetime
	                                         });
    }
}