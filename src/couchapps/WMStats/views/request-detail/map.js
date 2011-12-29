
function(doc) {
  if (doc.type == "reqmgr_request"){
    emit([doc._id, 1], doc) ;
  } else if (doc.type == "agent_request") {
    emit([doc.workflow, 0, doc.timestamp], doc);   
  }
}