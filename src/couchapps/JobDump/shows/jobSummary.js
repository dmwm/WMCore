function(doc, req) {
  if (doc == null) {
    return "Error: Unknown job: " + req.docId;
  }

  var response = "<html><head>\n";
  response += "<title>Summary for job " + doc["jobid"] + "</title>\n";
  response += "</head><body style=\"font-family: arial;\"><script src=../../jobSummary.js></script>\n";
  response += "<div id=\"info\"></div>\n";
  response += "<div id=\"mask\"></div>\n";
  response += "<div id=\"inputfiles\"></div>\n";
  response += "<div id=\"transitions\"></div>\n";
  response += "<div id=output></div><div id=errors></div><div id=logArchives></div>\n";

  response += "<script type=\"text/javascript\">\n";
  response += "renderJobSummary(" + doc["jobid"] + ");\n";
  response += "</script>\n";

  response += "</body></html>";
  return response;
}