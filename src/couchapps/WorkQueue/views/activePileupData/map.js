function(doc) {
  var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
  if (ele && ele["Status"] === "Available") {
      for (var pileupName in ele.PileupData) {
          if (ele.PileupData.hasOwnProperty(pileupName)) {
              emit([ele["Dbs"], inputName, ele.PileupData[pileupName]], doc['_id']);
          }
      }
  }
}