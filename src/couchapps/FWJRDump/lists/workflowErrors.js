function(head, req) {
  function printFailed(jobs, inputLFNs, inputRuns) {
    if (jobs.length == 0) {
      return;
    }

    var jobSummaryURL = "../../_show/jobSummary/";
    var uniqueJobs = new Array();
    var uniqueLFNs = new Array();
    jobs.sort();
    inputLFNs.sort();

    var previousJob = null;
    for (jobIndex in jobs) {
      if (previousJob == null || jobs[jobIndex] != previousJob) {
        uniqueJobs.push(jobs[jobIndex]);
        previousJob = jobs[jobIndex];
      }
    }

    var previousLFN = null;
    for (inputIndex in inputLFNs) {
      if (previousLFN == null || inputLFNs[inputIndex] != previousLFN) {
        uniqueLFNs.push(inputLFNs[inputIndex]);
        previousLFN = inputLFNs[inputIndex];
      }
    }

    if (uniqueJobs.length == 1) {
      send("      " + uniqueJobs.length + " job failed ");
    } else {
      send("      " + uniqueJobs.length + " jobs failed ");
    }

    if (jobs.length == 1) {
      send(jobs.length + " time:\n        ");
    } else {
      send(jobs.length + " times:\n        ");
    }

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
    send("\n\n");

    if (uniqueLFNs.length == 1) {
      send("      " + uniqueLFNs.length + " file was used as input for ");
    } else {
      send("      " + uniqueLFNs.length + " files were used as input for ");
    }

    if (uniqueJobs.length == 1) {
      send("this job:\n");
    } else {
      send("these jobs:\n");
    }

    for (var lfnIndex in uniqueLFNs) {
      send("        " + uniqueLFNs[lfnIndex] + "\n");
    }

    send("\n      Run Information:\n");
    for (var runNumber in inputRuns) {
      send("        " + runNumber + ":");
      for (var lumiIndex in inputRuns[runNumber]) {
        send(" " + inputRuns[runNumber][lumiIndex]);
        }
      send("\n");
    }  
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
  var inputRuns = new Object;

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
      inputRuns = new Object;

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

    for (var inputIndex in row.value["input"]) {
      inputLFNs.push(row.value["input"][inputIndex]);
    }

    for (var runNumber in row.value["runs"]) {
      var runFound = false;
      for (var knownRun in inputRuns) {
        if (knownRun == runNumber) {
          runFound = true;
          break;
        }
      }

      if (runFound == false) {
        inputRuns[runNumber] = new Array();
      }

      for (var lumiIndex in row.value["runs"][runNumber]) {
        var lumiFound = false;
        lumiNumber = row.value["runs"][runNumber][lumiIndex];
        for (var knownLumiIndex in inputRuns[runNumber]) {
          if (lumiNumber == inputRuns[runNumber][knownLumiIndex]) {
            lumiFound = true;
            break;
          }
        }

        if (lumiFound == false) {
          inputRuns[runNumber].push(lumiNumber);
        }
      }
    }
  }

  printFailed(jobs, inputLFNs, inputRuns);
  send("</pre></body></html>");
};
