function(doc, req) {
  var response = "<html><head>\n";
  response += "<title>Summary of cooloffs jobs at " + req.id + "</title>\n";
  response += "</head><body style=\"font-family: arial;\">\n";
  response += "<script src=../../cooloffSummary.js></script>\n";
  response += "<div id=errors></div>\n";

  response += "<script type=\"text/javascript\">\n";
  response += "xmlhttp = new XMLHttpRequest();\n";
  response += "renderSiteCooloffs(\"" + req.id + "\", document.getElementById(\"errors\"))\n";
  response += "</script>\n";
  response += "</body></html>";
  return response;
}
