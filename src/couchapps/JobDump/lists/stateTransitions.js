function(head, req) {
  var row;
  start({"headers": {"Content-Type": "text/html"}});

  send("<html><body><pre>\n");
  send("State Transitions:\n");

  var retryCount = 0;
  var failureSeen = false;
  var successSeen = false;

  while (row = getRow()) {
    if (row.value["newstate"] == "success") {
      successSeen = true;
    } else if  (row.value["newstate"] == "exhausted") {
      failureSeen = true;
    } else if (row.value["newstate"] == "jobcooloff") {
      retryCount += 1;
    }

    var transitionTimestamp = new Date(row.value["timestamp"] * 1000);

    if (row.value["location"] == "Agent") {
      send("  " + transitionTimestamp.toDateString() + " " +
           transitionTimestamp.toLocaleTimeString() + " " +
           row.value["oldstate"] + " -> " + row.value["newstate"] + "\n");
    } else {
      send("  " + transitionTimestamp.toDateString() + " " +
           transitionTimestamp.toLocaleTimeString() + " " +
           row.value["oldstate"] + " -> " + row.value["newstate"] + 
           " (" + row.value["location"] + ")\n");
    }
  }

  send("\nRetries: " + retryCount + "\n");
  if (successSeen) {
    send("Outcome: Success\n");
  } else if (failureSeen) {
    send("Outcome: Failure\n");
  } else {
    send("Outcome: Unknown, still running.");
  }

  send("</pre></body></html>");
};
