
function(doc) {
  if (doc.type == "reqmgr_request"){
    emit([doc.request_date, doc.requestor], {"campaign": doc.campaign, "status": doc.request_status[doc.request_status.length - 1].status}) ;
  } 
}
