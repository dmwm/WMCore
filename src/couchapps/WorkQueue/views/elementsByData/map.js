function(doc, site) {
  var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
  if (ele && ele["Status"] === "Available") {
    for (var i in ele.Inputs) {
      emit(i, {_id: doc._id});
    }
  }
}