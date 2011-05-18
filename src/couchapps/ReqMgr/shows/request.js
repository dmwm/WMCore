function(doc, req) {  
  var response = "<html><head>\n";
  response += "<title>Request " + doc.RequestName + "</title>\n";
  response += "</head><body style=\"font-family: arial;\">";
  response += "<table><tr><td></td></tr></table>";
  response += "</body></html>";
  return response;
  
}
