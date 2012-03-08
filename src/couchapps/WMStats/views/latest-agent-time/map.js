
function(doc) {
  if (doc.type == "agent_request") {
     if (doc.sites) {
        for (var site in doc.sites) {
            emit([doc.agent_url, site], doc.timestamp);
        }
     }
  } 
}