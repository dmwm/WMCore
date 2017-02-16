function(doc) {
   var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
   if (ele && ele['Jobs']) {
       // assumes request name and input data is one to one (None for MC workflow)
       if (ele['Status'] == 'Available' || ele['Status'] == 'Negotiating') {
           emit([ele["RequestName"], ele['Inputs'], 'inQueue'], ele['Jobs']);
       } else {
           emit([ele["RequestName"], ele['Inputs'], 'inWMBS'], ele['Jobs'])
       }
   }
}