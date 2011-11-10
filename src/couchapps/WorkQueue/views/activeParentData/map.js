function(doc) {
  var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
  if (ele && ele["Status"] === "Available" && ele["ParentData"]) {
    for (var i in ele.ParentData) {
        emit([ele.Dbs, i], null);
    }
  }
}