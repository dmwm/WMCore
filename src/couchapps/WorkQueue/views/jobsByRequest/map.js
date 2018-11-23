function(doc) {
   var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
   if (ele && ele['Jobs']) {
       emit(ele["RequestName"], ele["Jobs"]);
   }
}