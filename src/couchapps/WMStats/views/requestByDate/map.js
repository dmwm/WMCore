
function(doc) {
  if (doc.type == "reqmgr_request"){
    emit([doc.request_date, doc.requestor], null) ;
  } 
}
