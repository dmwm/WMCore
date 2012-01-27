function(keys, values, rereduce) {
  
  var statusObj = {
    "queued": {"first": 0, "retry": 0},
    "submitted": {"first": 0, "retry": 0, "pending": 0, "running": 0},
    "failure": {"create": 0, "submit": 0, "exception": 0},
    "canceled": 0,
    "success": 0,
    "cooloff": 0
    }
  
    
  function addJobs(base, additionList) {
    for (var status in additionList) {
      if (typeof(additionList[status]) == 'number') {
          base[status] += additionList[status];
      } else {
          addJobs(base[status], additionList[status])
      }
    }
  };
  
  for (var index in values) {
      addJobs(statusObj, values[index]);
  };
  
  return statusObj;
}