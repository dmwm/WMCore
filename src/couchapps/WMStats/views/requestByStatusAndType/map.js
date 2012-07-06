function(doc) {
  if (doc.type == "reqmgr_request"){
    emit([doc.workflow, doc.request_status[doc.request_status.length - 1].status, doc.request_type], null);
  }
}