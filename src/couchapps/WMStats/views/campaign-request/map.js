
function(doc) {
  if (doc.type == "reqmgr_request"){
    emit(doc.campaign, {'request_status': doc.request_status[doc.request_status.length() - 1].status, 
                        'status': doc.status}) ;
  } 
}