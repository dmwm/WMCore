function(doc, req) {
  if (doc == null) {
    return "Error: Unknown job: " + req.docId;
  }

  var response = "<html><head>\n";
  response += "<title>Summary for job " + doc["jobid"] + "</title>\n";
  response += "</head><body><pre>\n";
  response += "Name: " + doc["name"] + "\n";
  response += "Owner: " + doc["owner"] + "\n";
  response += "Workflow: " + doc["workflow"] + "\n";
  response += "Task: " + doc["task"] + "\n";

  response += "\nMask:\n";
  response += "  First Event: " + doc["mask"]["firstevent"] + "\n";
  response += "  Last Event: " + doc["mask"]["lastevent"] + "\n";
  response += "  First Lumi: " + doc["mask"]["firstlumi"] + "\n";
  response += "  Last Lumi: " + doc["mask"]["lastlumi"] + "\n";
  response += "  First Run: " + doc["mask"]["firstrun"] + "\n";
  response += "  Last Run: " + doc["mask"]["lastrun"] + "\n";

  response += "\nInput Files:\n";
  for (var inputFileIndex in doc["inputfiles"]) {
    var inputFile = doc["inputfiles"][inputFileIndex];
    response += "  " + inputFile["lfn"] + "\n";

    for(var inputFileParentIndex in inputFile["parents"]) {
      var inputFileParent = inputFile["parents"][inputFileParentIndex];
      response += "    " + inputFileParent["lfn"] + "\n";
    }
  }

  response += "\n";
  response += "<div id=stateTransitions></div>\n";

  response += "\n<br>\n";
  response += "<div id=errors></div>\n";

  response += "\n<br>\n";
  response += "<div id=logArchives></div>\n";

  response += "<script type=\"text/javascript\">\n";
  response += "xmlhttp = new XMLHttpRequest();\n";
  response += "xmlhttp.open(\"GET\", \"../../_list/stateTransitions/stateTransitionsByJobID?startkey=[" + doc["jobid"] + "]&endkey=[" + doc["jobid"] + ",{}]\", false);\n";
  response += "xmlhttp.send();\n";
  response += "document.getElementById(\"stateTransitions\").innerHTML=xmlhttp.responseText;\n";

  response += "xmlhttp.open(\"GET\", \"../../_list/jobErrors/errorsByJobID?startkey=[" + doc["jobid"] + "]&endkey=[" + doc["jobid"] + ",{}]\", false);\n";
  response += "xmlhttp.send();\n";
  response += "document.getElementById(\"errors\").innerHTML=xmlhttp.responseText;\n";

  response += "xmlhttp.open(\"GET\", \"../../_list/jobLogArchives/logArchivesByJobID?startkey=[" + doc["jobid"] + "]&endkey=[" + doc["jobid"] + ",{}]\", false);\n";
  response += "xmlhttp.send();\n";
  response += "document.getElementById(\"logArchives\").innerHTML=xmlhttp.responseText;\n";

  response += "</script>\n";

  response += "\n\n</pre></body></html>";
  return response;
}