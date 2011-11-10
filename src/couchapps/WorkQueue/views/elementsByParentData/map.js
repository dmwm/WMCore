function(doc) {
  var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
  if (ele && ele["Status"] === "Available" && ele.ParentFlag) {
    for (var i in ele.ParentData) {
      emit(i, {'_id' : doc['_id']});
    }
  }
}