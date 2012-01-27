
function(doc) {
  if (doc.type == "agent_request") {
     emit(doc.agent_url, doc.timestamp);
  } 
}