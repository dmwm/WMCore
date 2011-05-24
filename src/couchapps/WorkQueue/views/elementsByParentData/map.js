function(doc) {
  var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
  if (ele && ele["Status"] === "Available" && ele.ParentFlag === "True") {
    for (var i in ele.ParentData) {
      emit(ele.ParentData[i].Name, {'_id' : doc['_id']});
    }
  }
}