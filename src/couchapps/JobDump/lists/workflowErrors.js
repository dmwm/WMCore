function(head, req) {
  function printFailed(jobs, inputLFNs) {
    if (jobs.length == 0) {
      return;
    }

    var jobSummaryURL = "../../_show/jobSummary/";
    var uniqueJobs = new Array();
    jobs.sort();

    var previousJob = null;
    for (jobIndex in jobs) {
      if (previousJob == null || jobs[jobIndex] != previousJob) {
        uniqueJobs.push(jobs[jobIndex]);
        previousJob = jobs[jobIndex];
      }
    }
         
    send("      " + uniqueJobs.length + " jobs failed " + jobs.length + " times:\n        ");
    var first = true;
    var jobCount = 0;
    for (jobIndex in uniqueJobs) {
      if (first) {
        send("<a href=" + jobSummaryURL + uniqueJobs[jobIndex] + ">" + uniqueJobs[jobIndex] + "</a>");
        jobCount += 1;
        first = false;
      } else if (jobCount == 7) {
        send(",\n        <a href=" + jobSummaryURL + uniqueJobs[jobIndex] + ">" + uniqueJobs[jobIndex] + "</a>");
        jobCount = 0;
      } else {
        send(", <a href=" + jobSummaryURL + uniqueJobs[jobIndex] + ">" + uniqueJobs[jobIndex] + "</a>");
        jobCount += 1;
      }      
    }
    send("\n");
  }

  var row;
  start({"headers": {"Content-Type": "text/html"}});

  send("<html><body><pre>\n");
  send("Failures:\n");

  var taskName = "None";
  var stepName = "None";
  var errorDesc = "None";
  var jobs = new Array();
  var inputLFNs = new Array();

  while (row = getRow()) {
    if (taskName != row.value["task"]) {
      printFailed(jobs, inputLFNs);
      jobs = new Array();
      inputLFNs = new Array();

      taskName = row.value["task"];
      stepName = "None";
      errorDesc = "None";
      send("\n  " + taskName + ":\n");
    }

    if (stepName != row.value["step"]) {
      printFailed(jobs, inputLFNs);
      jobs = new Array();
      inputLFNs = new Array();

      stepName = row.value["step"];
      errorDesc = "None";
      send("    " + stepName + ":\n");
    }

    if (errorDesc != row.value["error"][0]["details"]) {
      printFailed(jobs, inputLFNs);
      jobs = new Array();
      inputLFNs = new Array();

      errorDesc = row.value["error"][0]["details"];
      for (errorIndex in row.value["error"]) {
        jobError = row.value["error"][errorIndex];
        send("      Error Type: " + jobError["type"] + "\n");
        send("      Error Code: " + jobError["exitCode"] + "\n");
        send("      Error Details:\n");
        errorLines = jobError["details"].split("\n");
        for (var errorLineIndex in errorLines) {
          send("        " + errorLines[errorLineIndex] + "\n");
        }
        send("\n");
      }
    }

    jobs.push(row.value["jobid"]);
  }

  printFailed(jobs, inputLFNs);
  send("</pre></body></html>");
};
