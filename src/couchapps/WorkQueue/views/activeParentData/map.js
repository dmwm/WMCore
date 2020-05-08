function(doc) {
  var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
  if (ele && ele["Status"] === "Available") {
      for (var parentName in ele.ParentData) {
          if (ele.ParentData.hasOwnProperty(parentName)) {
              emit([ele["Dbs"], inputName, ele.ParentData[parentName]], doc['_id']);
          }
      }
  }
}