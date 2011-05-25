function(doc, req) {
  var response = "<html><head>\n";
  response += "<title>Summary for workflow " + req.id + "</title>\n";
  response += "</head><body style=\"font-family: arial;\">\n";
  response += "<script src=../../workflowSummary.js></script>\n";
  response += "Output datasets:\n<br>\n";
  response += "<div id=output style=\"margin: 0px 0px 0px 15px;\"></div>\n";

  response += "\n<br>\n";
  response += "Missed Run/Lumis:<br>\n\n";
  response += "<div id=runlumi></div><br>\n";
  response += "Failures:\n\n";
  response += "<div id=errors></div>\n";

  response += "<script type=\"text/javascript\">\n";
  response += "xmlhttp = new XMLHttpRequest();\n";
  response += "renderWorkflowErrors(\"" + req.id + "\", document.getElementById(\"errors\"))\n";
  response += "xmlhttp.open(\"GET\", \"../../_list/workflowOutput/outputByWorkflowName?stale=ok&group=true&group_level=2&startkey=[\\\"" + req.id + "\\\"]&endkey=[\\\"" + req.id + "\\\",{}]\", false);\n";
  response += "xmlhttp.send();\n";
  response += "document.getElementById(\"output\").innerHTML=xmlhttp.responseText;\n";

  response += "</script>\n";
  response += "</body></html>";
  return response;
}
