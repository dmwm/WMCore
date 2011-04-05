function(doc, site) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    ele.InsertTime = doc.timestamp
    ele.UpdateTime = doc.updatetime
    emit([ele.RequestName, ele.Status], ele);
}