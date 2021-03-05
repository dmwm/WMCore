function(doc, site) {
  var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
  if (ele && ele["Status"] === "Available") {
    for (var inputName in ele.Inputs) {
        if (ele.Inputs.hasOwnProperty(inputName)) {
            emit([ele["Dbs"], inputName, ele.Inputs[inputName]], doc['_id']);
        }
    }
  }
}