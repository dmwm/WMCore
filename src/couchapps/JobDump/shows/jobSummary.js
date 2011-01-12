function(doc, req) {
  if (doc == null) {
    return "Error: Unknown job: " + req.docId;
  }

  var response = "<html><head>\n";
  response += "<title>Summary for job " + doc["jobid"] + "</title>\n";
  response += "</head><body><script src=../../jobSummary.js></script><pre>\n";
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
  response += "<div id=\"inputfiles\"></div>\n";
  response += "<script type=\"text/javascript\">\n";
  response += "var inputFiles = new Array();\n";

  for (var inputFileIndex in doc["inputfiles"]) {
    var inputFile = doc["inputfiles"][inputFileIndex];
    response += "inputFiles.push(\"" + inputFile["lfn"] + "\");\n";
  }

  response += "renderJobInputFiles(\"" + doc["workflow"] + "\", inputFiles, " + doc["jobid"] + ", document.getElementById(\"inputfiles\"));\n";
  response += "</script>\n";
  response += "\nState Transitions:\n";
  for (var transitionIndex in doc["states"]) {
    var transition = doc["states"][transitionIndex];
    var transitionTimestamp = new Date(transition["timestamp"] * 1000);

    if (transition["location"] == "Agent") {
      response += "  " + transitionTimestamp.toDateString() + " ";
      response += transitionTimestamp.toLocaleTimeString() + " ";
      response += transition["oldstate"] + " -> " + transition["newstate"] + "\n";
    } else {
      response += "  " + transitionTimestamp.toDateString() + " ";
      response += transitionTimestamp.toLocaleTimeString() + " ";
      response += transition["oldstate"] + " -> " + transition["newstate"];
      response += " (" + transition["location"] + ")\n";
    }
  }

  response += "\n";
  response += "<div id=output></div>\n";

  response += "\n<br>\n";
  response += "<div id=errors></div>\n";

  response += "\n<br>\n";
  response += "<div id=logArchives></div>\n";

  response += "<script type=\"text/javascript\">\n";
  response += "xmlhttp = new XMLHttpRequest();\n";

  response += "xmlhttp.open(\"GET\", \"../../_list/jobOutput/outputByJobID?stale=ok&startkey=" + doc["jobid"] + "&endkey=" + doc["jobid"] + "\", false);\n";
  response += "xmlhttp.send();\n";
  response += "document.getElementById(\"output\").innerHTML=xmlhttp.responseText;\n";

  response += "xmlhttp.open(\"GET\", \"../../_list/jobErrors/errorsByJobID?stale=ok&startkey=[" + doc["jobid"] + "]&endkey=[" + doc["jobid"] + ",{}]\", false);\n";
  response += "xmlhttp.send();\n";
  response += "document.getElementById(\"errors\").innerHTML=xmlhttp.responseText;\n";

  response += "xmlhttp.open(\"GET\", \"../../_list/jobLogArchives/logArchivesByJobID?stale=ok&startkey=[" + doc["jobid"] + "]&endkey=[" + doc["jobid"] + ",{}]\", false);\n";
  response += "xmlhttp.send();\n";
  response += "document.getElementById(\"logArchives\").innerHTML=xmlhttp.responseText;\n";

  response += "</script>\n";

  response += "\n\n</pre></body></html>";
  return response;
}