function(keys, values, rereduce) {
  
  /*
   statusObj structure
  statusObj = {
    "queued": {"first": 0, "retry": 0},
    "submitted": {"first": 0, "retry": 0, "pending": 0, "running": 0},
    "failure": {"create": 0, "submit": 0, "exception": 0},
    "canceled": 0,
    "success": 0,
    "cooloff": 0
    }
  */
  var statusObj = {};
    
  function addJobs(base, additionList) {
    for (var status in additionList) {
      if (typeof(additionList[status]) == 'number') {
          if (base[status] === undefined) {base[status] = 0;}
          base[status] += additionList[status];
      } else {
          if (base[status] === undefined) {base[status] = {};}
          addJobs(base[status], additionList[status])
      }
    }
  };
  
  for (var index in values) {
      addJobs(statusObj, values[index]);
  };
  
  return statusObj;
}