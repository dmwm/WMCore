function(doc) {
   var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
   if (ele && ele['Jobs']) {
       if (ele["Jobs"] === null) {
            emit(ele["RequestName"], 0);
       } else {
            emit(ele["RequestName"], ele["Jobs"]);
       }
   }
}