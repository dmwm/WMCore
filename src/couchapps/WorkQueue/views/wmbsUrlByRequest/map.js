function(doc) {
   var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
   if (ele['WMBSURL']) {
       emit([ele["RequestName"], ele['WMBSURL']], null);
   };
}