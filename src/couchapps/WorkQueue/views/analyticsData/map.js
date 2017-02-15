function(doc) {
   var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
   if (ele && ele['Jobs'] >= 0) {
       var jobs = ele['Jobs']
       if (ele["Jobs"] === null)
           jobs = 0
       // assumes request name and input data is one to one (None for MC workflow)
       if (ele['Status'] == 'Available' || ele['Status'] == 'Negotiating') {
           emit([ele["RequestName"], ele['Inputs'], 'inQueue'], jobs);
       } else {
           emit([ele["RequestName"], ele['Inputs'], 'inWMBS'], jobs)
       }
   }
}