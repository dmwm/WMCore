function(head, req) {
  var row;
  start({"headers": {"Content-Type": "text/html"}});

  send("<html><body><pre>\n");
  send("Errors:");

  while (row = getRow()) {
    send("\nRetry " + row.value["retry"] + ", step " + row.value["step"] + " failed:\n");

    for (var errorIndex in row.value["error"]) {
      var jobError = row.value["error"][errorIndex];
      send("  Type: " + jobError["type"] + "\n");
      send("  Details: \n");
      errorLines = jobError["details"].split("\n");
      for (var errorLineIndex in errorLines) {
        send("    " + errorLines[errorLineIndex] + "\n");
      }
      send("  Exit Code: " + jobError["exitCode"] + "\n\n");
    }  
  }

  send("</pre></body></html>");
};
