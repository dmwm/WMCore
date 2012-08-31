function(doc) {
  if (doc.type == "agent_request"){
    emit([doc.timestamp, doc.workflow], null) ;
  } 
}