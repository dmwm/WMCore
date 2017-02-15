function(doc) {
   var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
   if (ele && ele['Jobs'] >= 0) {
       var jobs = ele['Jobs']
       if (ele["Jobs"] === null)
           jobs = 0
       if (ele['Status'] == 'Available' || ele['Status'] == 'Negotiating') {
           emit([ele["RequestName"], 'inQueue'], jobs);
       } else {
           emit([ele["RequestName"], 'inWMBS'], jobs)
       }
   }
}