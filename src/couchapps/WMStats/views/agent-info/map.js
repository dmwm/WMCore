
function(doc) {
  if (doc.type == "agent_info") {
    emit(doc.url, doc);   
  }
}