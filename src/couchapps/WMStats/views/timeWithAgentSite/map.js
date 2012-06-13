
function(doc) {
  if (doc.type == "agent_request") {
      for (var site in doc.sites) {
        emit([doc.timestamp, site, doc.agent_url], doc.sites[site]);
      }
  } 
}