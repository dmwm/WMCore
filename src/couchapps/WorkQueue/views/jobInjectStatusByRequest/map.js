function(doc) {
   var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
   if (ele && ele['Jobs']) {
       if (ele['Status'] == 'Available' || ele['Status'] == 'Negotiating') {
           emit([ele["RequestName"], 'inQueue'], ele['Jobs']);
       } else {
           emit([ele["RequestName"], 'inWMBS'], ele['Jobs'])
       }
   }
}