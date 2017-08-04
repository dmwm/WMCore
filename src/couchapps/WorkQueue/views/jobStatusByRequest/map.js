function(doc) {
   var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
   if (ele && ele['Jobs'] !== undefined) {
       emit([ele["RequestName"], ele['Status']], ele['Jobs']);
   }
}