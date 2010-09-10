function(doc, req) {
  var response = "<html><head>\n";
  response += "<title>Summary for workflow " + req.docId + "</title>\n";
  response += "</head><body><pre>\n";

  response += "Output datasets:\n";
  response += "<iframe scrolling=auto frameborder=0 width=100% marginwidth=0 marginheight=0 src='../../_list/workflowOutput/outputByWorkflowName?group=true&group_level=2&startkey=[\"" + req.docId + "\"]&endkey=[\"" + req.docId + "\",{}]'>";
  response += "</iframe>\n";

  response += "<br>\n";
  response += "<iframe scrolling=auto frameborder=0 width=100% marginwidth=0 marginheight=0 src='../../_list/workflowErrors/errorsByWorkflowName?startkey=[\"" + req.docId + "\"]&endkey=[\"" + req.docId + "\",{}]'>";
  response += "</iframe>\n";

  response += "\n\n</pre></body></html>";
  return response;
}