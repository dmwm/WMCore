
function(doc) {
  if (doc.type == "agent_request") {
      for (var site in doc.sites) {
        emit([doc.timestamp, doc.agent_url, site], doc.sites[site]);
      }
  } 
}