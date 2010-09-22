function(doc, req) {
  var response = "<html><head>\n";
  response += "<title>Summary for workflow " + req.docId + "</title>\n";
  response += "</head><body><pre>\n";

  response += "Output datasets:\n<br>\n";
  response += "<div id=output></div>\n";

  response += "\n<br>\n";
  response += "<div id=errors></div>\n";

  response += "<script type=\"text/javascript\">\n";
  response += "xmlhttp = new XMLHttpRequest();\n";
  response += "xmlhttp.open(\"GET\", \"../../_list/workflowOutput/outputByWorkflowName?group=true&group_level=2&startkey=[\\\"" + req.docId + "\\\"]&endkey=[\\\"" + req.docId + "\\\",{}]\", false);\n";
  response += "xmlhttp.send();\n";
  response += "document.getElementById(\"output\").innerHTML=xmlhttp.responseText;\n";

  response += "xmlhttp.open(\"GET\", \"../../_list/workflowErrors/errorsByWorkflowName?startkey=[\\\"" + req.docId + "\\\"]&endkey=[\\\"" + req.docId + "\\\",{}]\", false);\n";
  response += "xmlhttp.send();\n";
  response += "document.getElementById(\"errors\").innerHTML=xmlhttp.responseText;\n";

  response += "</script>\n";
  response += "\n\n</pre></body></html>";
  return response;
}