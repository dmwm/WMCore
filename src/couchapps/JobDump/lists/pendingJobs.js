function(head, req) {
  var row;
  start({"headers": {"Content-Type": "text/html"}});

  var jobSummaryURL = "../../_show/jobSummary/";
  var currentTask = "None";
  var jobCount = 0;

  while (row = getRow()) {
    if(row.value["state"] != "pending") {
      continue;
    }
    if (currentTask == "None") {
      var specName = row.value["task"].split('/')[1];
      send("<html><head><title>Pending Jobs For " + specName + "</title></head><body><pre>\n");
    }
    if (row.value["task"] != currentTask) {      
      send("\n\n" + row.value["task"] + ":\n");
      send("  <a href=" + jobSummaryURL + row.value["jobid"] + ">" + row.value["jobid"] + "</a>");
      currentTask = row.value["task"];
      jobCount = 0;
    } else {
      if (jobCount == 7) {
        send(",\n  <a href=" + jobSummaryURL + row.value["jobid"] + ">" + row.value["jobid"] + "</a>");
        jobCount = 0;
      } else {
        send(", <a href=" + jobSummaryURL + row.value["jobid"] + ">" + row.value["jobid"] + "</a>");
        jobCount += 1;
      }
    }
  }

  send("</pre></body></html>");
};
