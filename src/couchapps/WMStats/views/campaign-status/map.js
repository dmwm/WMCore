
function(doc) {
  if (doc.type == "reqmgr_request"){
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

    var summary = {};
    summary[doc.request_status[doc.request_status.length - 1].status] = 1; 
    //summary['status'] = doc.status;
    emit(doc.campaign, summary) ;
  } 
}