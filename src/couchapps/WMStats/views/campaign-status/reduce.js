function(keys, values, rereduce) {
  var summary = {
    "new": 0,
    "testing-approved": 0,
    "testing": 0,
    "tested": 0,
    "test-failed": 0,
    "assignment-approved": 0,
    "assigned": 0,
    "ops-hold": 0,
    "negotiating": 0,
    "acquired": 0,
    "running": 0,
    "failed": 0,
    "epic-FAILED": 0,
    "completed": 0,
    "closed-out": 0,
    "announced": 0,
    "aborted": 0,
    "rejected": 0
    };
    
  for (var index in values) {
    for (var status in values[index]) {
      summary[status] += values[index][status];
    }
  }
  return summary;
}