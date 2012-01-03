
function(doc) {
  if (doc.type == "agent_info") {
    emit(doc.agent_url, doc);   
  }
}