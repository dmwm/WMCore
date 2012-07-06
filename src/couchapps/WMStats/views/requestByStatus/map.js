function(doc) {
  if (doc.type == "reqmgr_request"){
    emit(doc.request_status[doc.request_status.length - 1].status, {'id': doc._id});
  }
}