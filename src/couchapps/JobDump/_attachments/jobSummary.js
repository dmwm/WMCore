function getRootDBPath() {
  // Figure out the root DB path given the current url of the page.  This will
  // return just the hostname and the database name with /jobs or /fwjrs.
  var urlParts = location.href.split('/');
  var rootDBName = urlParts[3].split('%2F')[0];
  return "http://" + urlParts[2] + "/" + rootDBName;
}

function getJobDocument(jobID) {
  // Retrieve a job document given the jobID.
  xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", "../../../../" + jobID, false);
  xmlhttp.send();

  return eval("(" + xmlhttp.responseText + ")");
};

function getJobOutput(jobID, outputType) {
  // Retrieve the output of a job.
  xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", getRootDBPath() + "%2Ffwjrs/_design/FWJRDump/_view/" + outputType + "ByJobID?stale=ok&startkey=" + jobID + "&endkey=" + jobID , false);
  xmlhttp.send();

  return eval("(" + xmlhttp.responseText + ")");
};

function getParentJobForFile(requestName, jobID, fileLFN) {
  // Retrieve the ID of the job that produce this file.  If no parent job
  // exists, return null.
  xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", getRootDBPath() + "%2Ffwjrs/_design/FWJRDump/_view/jobsByOutputLFN?stale=ok&startkey=[\"" + requestName + "\",\"" + fileLFN + "\"]&endkey=[\"" + requestName + "\",\"" + fileLFN + "\",{}]", false);
  xmlhttp.send();

  var results = eval("(" + xmlhttp.responseText + ")")["rows"];
  if (results && results.length > 0 && results[0]["value"] != jobID) {
    return results[0]["value"];
  }

  return null;
};

function getSiblingJobsForFile(requestName, jobID, fileLFN) {
  // Retrieve the IDs of any jobs except the one in jobID that used this file as
  // input.
  xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", "../../_view/jobsByInputLFN?stale=ok&startkey=[\"" + requestName + "\",\"" + fileLFN + "\"]&endkey=[\"" + requestName + "\",\"" + fileLFN + "\",{}]", false);
  xmlhttp.send();

  results = eval("(" + xmlhttp.responseText + ")")["rows"];
  var siblingJobIDs = new Array();
  for (resultIndex in results) {
    if (results[resultIndex]["value"] != jobID) {
      siblingJobIDs.push(results[resultIndex]["value"]);
    };
  };

  return siblingJobIDs;
};

function renderJobFiles(requestName, files, jobID, div) {
  // Render information about the array of files passed into this function.
  // If the jobID parameter is not null this will list the job that output
  // a particular file.  
  if (files.length == 0) {
    div.innerHTML = "(none)";
    return
  };

  for (var fileIndex in files) {
    var fileDiv = document.createElement("div");
    fileDiv.style.margin = "3px 0px 0px 10px"; 
    fileDiv.innerHTML = files[fileIndex]["lfn"];
    div.appendChild(fileDiv);

    var parentSiblingDiv = document.createElement("div");
    parentSiblingDiv.style.margin = "0px 0px 0px 20px";
    fileDiv.appendChild(parentSiblingDiv);

    var parentJob = getParentJobForFile(requestName, jobID, files[fileIndex]["lfn"]);

    if (parentJob) {
      parentSiblingDiv.innerHTML = "Produced by: <a href=\"../jobSummary/" + parentJob + "\">" + parentJob + "</a>";
    };

    var siblingJobs = getSiblingJobsForFile(requestName, jobID, files[fileIndex]);
    if (siblingJobs.length > 0) {
      parentSiblingDiv.innerHTML += " Used by: ";

      for (var siblingJobIndex in siblingJobs) {
        parentSiblingDiv.innerHTML += "<a href=\"../jobSummary/" + siblingJobs[siblingJobIndex] + "\">" + siblingJobs[siblingJobIndex] + "</a>";
        if (siblingJobIndex != siblingJobs.length - 1) {
          parentSiblingDiv.innerHTML += ", ";
        };
      };
    };
  };
};

function renderJobOutputFiles(requestName, jobID) {
  // Render information about the array of files passed into this function.
  // If the jobID parameter is not null this will list the job that output
  // a particular file.
  jobOutput = getJobOutput(jobID, "output");
  outputLabelDiv = document.getElementById("output");
  outputLabelDiv.innerHTML = "Output Files:"
  outputLabelDiv.style.margin = "10px 0px 0px 0px";
  outputDiv = document.createElement("div");
  outputDiv.style.margin = "0px 0px 0px 15px";
  outputLabelDiv.appendChild(outputDiv);

  if (jobOutput["rows"].length == 0) {
    outputDiv.innerHTML = "(none)";
    return
  };

  for (var fileIndex in jobOutput["rows"]) {
    var fileDiv = document.createElement("div");
    fileDiv.style.margin = "3px 0px 0px 10px"; 
    fileDiv.innerHTML = jobOutput["rows"][fileIndex]["value"]["lfn"];
    outputDiv.appendChild(fileDiv);

    var parentSiblingDiv = document.createElement("div");
    parentSiblingDiv.style.margin = "0px 0px 0px 20px";
    fileDiv.appendChild(parentSiblingDiv);

    var parentJob = getParentJobForFile(requestName, jobID, jobOutput["rows"][fileIndex]["value"]["lfn"]);

    if (parentJob) {
      parentSiblingDiv.innerHTML = "Produced by: <a href=\"../jobSummary/" + parentJob + "\">" + parentJob + "</a>";
    };

    var siblingJobs = getSiblingJobsForFile(requestName, jobID, jobOutput["rows"][fileIndex]["value"]["lfn"]);
    if (siblingJobs.length > 0) {
      parentSiblingDiv.innerHTML += " Used by: ";

      for (var siblingJobIndex in siblingJobs) {
        parentSiblingDiv.innerHTML += "<a href=\"../jobSummary/" + siblingJobs[siblingJobIndex] + "\">" + siblingJobs[siblingJobIndex] + "</a>";
        if (siblingJobIndex != siblingJobs.length - 1) {
          parentSiblingDiv.innerHTML += ", ";
        };
      };
    };
  };
};

function renderJobMetaData(jobDocument) {
  // Add information about the job including the name, owner, task
  // and workflow to the summary page.
  infoDiv = document.getElementById("info");
  infoDiv.innerHTML += "Name: " + jobDocument["name"] + "<br>";
  infoDiv.innerHTML += "Owner: " + jobDocument["owner"] + "<br>";
  infoDiv.innerHTML += "Workflow: " + jobDocument["workflow"] + "<br>";
  infoDiv.innerHTML += "Task: " + jobDocument["task"] + "<br>";
};

function renderJobMask(jobDocument) {
  // Add information from the job mask to the summary page.
  var maskLabelDiv = document.getElementById("mask");
  maskLabelDiv.innerHTML = "Mask:\n";
  maskLabelDiv.style.margin = "10px 0px 0px 0px";

  maskDiv = document.createElement("div");
  maskDiv.style.margin = "0px 0px 0px 15px";

  if(jobDocument["mask"]["FirstEvent"] != null) {
    maskDiv.innerHTML += "events: " + jobDocument["mask"]["FirstEvent"] + " - " +
      jobDocument["mask"]["LastEvent"] + "<br>\n";
  } else if (jobDocument["mask"]["FirstLumi"] != null) {
    maskDiv.innerHTML += "lumis: " + jobDocument["mask"]["FirstRun"] + ":" +
      jobDocument["mask"]["FirstLumi"] + " - " + jobDocument["mask"]["LastRun"] +
      ":" + jobDocument["mask"]["LastLumi"] + "<br>\n";
  } else if (jobDocument["mask"]["FirstRun"] != null) {
    maskDiv.innerHTML += "runs: " + jobDocument["mask"]["FirstRun"] + " - " +
       jobDocument["mask"]["LastRun"] + "<br>\n";
  } else if (jobDocument["mask"].runAndLumis) {
    for (runNumber in jobDocument["mask"]["runAndLumis"]) {
      maskDiv.innerHTML += runNumber + ": ";
      for (maskIndex in jobDocument["mask"]["runAndLumis"][runNumber]) {
        lumiMask = jobDocument["mask"]["runAndLumis"][runNumber][maskIndex];
        if (parseInt(maskIndex) + 1 == jobDocument["mask"]["runAndLumis"][runNumber].length) {
          maskDiv.innerHTML += "[" + lumiMask[0] + ", " + lumiMask[1] + "]\n";
        } else {
          maskDiv.innerHTML += "[" + lumiMask[0] + ", " + lumiMask[1] + "], ";
        }
      }
      maskDiv.innerHTML += "<br>\n";
    }
  } else {
    maskDiv.innerHTML += "(none)<br>\n";
  }

  maskLabelDiv.appendChild(maskDiv);
};

function renderJobTransitions(jobDocument) {
  // Render all of the state transitions that the job has made.
  var transitionLabelDiv = document.getElementById("transitions");
  transitionLabelDiv.innerHTML = "State Transitions:\n";
  transitionLabelDiv.style.margin = "10px 0px 0px 0px";

  transitionDiv = document.createElement("div");
  transitionDiv.style.margin = "0px 0px 0px 15px";

  for (var transitionIndex in jobDocument["states"]) {
    var transition = jobDocument["states"][transitionIndex];
    var transitionTimestamp = new Date(transition["timestamp"] * 1000);

    if (transition["location"] == "Agent") {
      transitionDiv.innerHTML += "  " + transitionTimestamp.toUTCString() + " ";
      transitionDiv.innerHTML += transition["oldstate"] + " -> " + transition["newstate"] + "<br>";
    } else {
      transitionDiv.innerHTML += "  " + transitionTimestamp.toUTCString() + " ";
      transitionDiv.innerHTML += transition["oldstate"] + " -> " + transition["newstate"];
      transitionDiv.innerHTML += " (" + transition["location"] + ")<br>";
    }
  }

  transitionLabelDiv.appendChild(transitionDiv);
};

function renderErrorDetails(errorsDiv, errorInfo) {
  // Insert the error information into the given div.
  for(var errorIndex in errorInfo["error"]) {
    stepError = errorInfo["error"][errorIndex];
    var errorDiv = document.createElement("div");

    if (errorIndex == 0) {
      errorDiv.style.margin = "5px 0px 0px 15px";
    } else {
      errorDiv.style.margin = "10px 0px 0px 15px";
    }

    errorDiv.innerHTML = "<b>Retry:</b>" + errorInfo["retry"] + "<br>";
    errorDiv.innerHTML += "<b>Type:</b> " + stepError["type"] + "<br>";
    errorDiv.innerHTML += "<b>Exit Code:</b> " + stepError["exitCode"] + "<br>";
    errorDiv.innerHTML += "<b>Details:</b>";
    errorsDiv.appendChild(errorDiv);

    var errorDetailsPre = document.createElement("pre");
    errorDetailsPre.style.margin = "0px 0px 0px 15px";
    errorDetailsPre.style.backgroundColor = "silver";
    errorDetailsPre.style.marginTop = "0px";
    errorDetailsPre.style.marginBottom = "0px";
    errorDetailsPre.style.width = "100%";
    errorDetailsPre.innerHTML = stepError["details"];
    errorDiv.appendChild(errorDetailsPre);
  };
};

function renderJobErrors(jobID) {
  // Retrieve and render all of the errors from a given job.
  errorsLabelDiv = document.getElementById("errors");
  errorsLabelDiv.innerHTML = "Errors:"
  errorsLabelDiv.style.margin = "10px 0px 0px 0px";
  errorsDiv = document.createElement("div");
  errorsDiv.style.margin = "0px 0px 0px 15px";
  errorsLabelDiv.appendChild(errorsDiv);

  xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", getRootDBPath() + "%2Ffwjrs/_design/FWJRDump/_view/errorsByJobID?stale=ok&startkey=[" + jobID + "]&endkey=[" + jobID + ",{}]", false);
  xmlhttp.send();
  jobErrors = eval("(" + xmlhttp.responseText + ")");

  if (jobErrors["rows"].length == 0) {
    errorsDiv.innerHTML = "(none)";
    return;
  };

  for(rowIndex in jobErrors["rows"]) {
    renderErrorDetails(errorsDiv, jobErrors["rows"][rowIndex]["value"]);
  };
};

function renderLogArchives(jobID, requestName) {
  // Render all of the logarchive information for the given job.
  xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", getRootDBPath() + "%2Ffwjrs/_design/FWJRDump/_view/logArchivesByJobID?stale=ok&startkey=[" + jobID + "]&endkey=[" + jobID + ",{}]", false);
  xmlhttp.send();
  jobArchives = eval("(" + xmlhttp.responseText + ")");

  archivesLabelDiv = document.getElementById("logArchives");
  archivesLabelDiv.innerHTML = "Log Archives:"
  archivesLabelDiv.style.margin = "10px 0px 0px 0px";

  if (jobArchives["rows"].length == 0) {
    archivesDiv = document.createElement("div");
    archivesDiv.style.margin = "0px 0px 0px 15px";
    archivesLabelDiv.appendChild(archivesDiv);
    archivesDiv.innerHTML = "(none)";
    return;
  };

  for (archiveIndex in jobArchives["rows"]) {
    archivesDiv = document.createElement("div");
    archivesDiv.style.margin = "0px 0px 0px 15px";
    archivesLabelDiv.appendChild(archivesDiv);
    archivesDiv.innerHTML += "Retry ";
    archivesDiv.innerHTML += jobArchives["rows"][archiveIndex]["value"]["retrycount"];
    archivesDiv.innerHTML += " -> " + jobArchives["rows"][archiveIndex]["value"]["lfn"];

    var siblingJobs = getSiblingJobsForFile(requestName, jobID, 
                                            jobArchives["rows"][archiveIndex]["value"]["lfn"]);
    if (siblingJobs.length > 0) {
      archivesParentSiblingDiv = document.createElement("div");
      archivesParentSiblingDiv.style.margin = "0px 0px 0px 15px";
      archivesDiv.appendChild(archivesParentSiblingDiv);
      archivesParentSiblingDiv.innerHTML += " Used by: ";

      for (var siblingJobIndex in siblingJobs) {
        archivesParentSiblingDiv.innerHTML += "<a href=\"../jobSummary/" + siblingJobs[siblingJobIndex] + "\">" + siblingJobs[siblingJobIndex] + "</a>";
        if (siblingJobIndex != siblingJobs.length - 1) {
          archivesParentSiblingDiv.innerHTML += ", ";
        };
      };
    };
  }
}

function renderJobSummary(jobID) {
  // Render the job summary.
  var jobDoc = getJobDocument(jobID);

  renderJobMetaData(jobDoc);
  renderJobMask(jobDoc);

  inputLabelDiv = document.getElementById("inputfiles");
  inputLabelDiv.innerHTML = "Input Files:"
  inputLabelDiv.style.margin = "10px 0px 0px 0px";
  inputDiv = document.createElement("div");
  inputDiv.style.margin = "0px 0px 0px 15px";
  renderJobFiles(jobDoc["workflow"], jobDoc["inputfiles"], jobID, inputDiv);
  inputLabelDiv.appendChild(inputDiv);

  renderJobTransitions(jobDoc);
  renderJobOutputFiles(jobDoc["workflow"], jobID);

  renderJobErrors(jobID);
  renderLogArchives(jobID, jobDoc["workflow"]);
};
