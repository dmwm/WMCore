function(doc) {
  var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
  if (ele && ele["Status"] === "Available" && ele["PileupData"]) {
    for (var i in ele["PileupData"]) {
      emit(i, {'_id' : doc['_id']});
    }
  }
}