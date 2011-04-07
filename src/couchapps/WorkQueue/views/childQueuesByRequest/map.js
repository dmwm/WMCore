function(doc) {
   var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
   if (ele && ele['ChildQueueUrl']) {
       emit([ele["RequestName"], ele['ChildQueueUrl']], null);
   }
}