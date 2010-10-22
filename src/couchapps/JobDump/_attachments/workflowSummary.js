function getFailedJobs(workflowName) {
  xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", "../../_view/failedJobsByWorkflowName?startkey=[\"" + workflowName + "\"]&endkey=[\"" + workflowName + "\",{}]", false);
  xmlhttp.send();
  return eval("(" + xmlhttp.responseText + ")")["rows"];
};

function getErrorInfoForJob(jobID) {
  xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", "../../_view/errorsByJobID?startkey=[" + jobID + "]&endkey=[" + jobID + ",{}]", false);
  xmlhttp.send();
  return eval("(" + xmlhttp.responseText + ")")["rows"];
};

function collateFailureInfo(failedJobs) {
  // first level - task name
  // second level - step name
  //  errors = [], {type, code, details}
  //  jobs = []
  //  input = []
  //  runs = {}, {run: [lumis]} 

  var workflowFailures = {};

  for (failedJobIndex in failedJobs) {
    var jobID = failedJobs[failedJobIndex]["value"];
    var jobErrors = getErrorInfoForJob(jobID);

    for (var errorIndex in jobErrors) {
      var workflowError = jobErrors[errorIndex]["value"]
      if (!workflowFailures.hasOwnProperty(workflowError["task"])) {
        workflowFailures[workflowError["task"]] = {};
      }

      var taskFailure = workflowFailures[workflowError["task"]]
      if (!taskFailure.hasOwnProperty(workflowError["step"])) {
        taskFailure[workflowError["step"]] = {"errors": new Array(),
                                              "jobs": new Array(),
                                              "input": new Array(),
                                              "runs": {}};
      };

      var stepFailure = taskFailure[workflowError["step"]];
      for (var fwjrErrorIndex in workflowError["error"]) {
        var fwjrError = workflowError["error"][fwjrErrorIndex];
        var errorExists = false;
        for (var detailsIndex in stepFailure["errors"]) {
          var errorDetails = stepFailure["errors"][detailsIndex]["details"];
          if (errorDetails == fwjrError["details"]) {
            errorExists = true;
            break;
          }
        }

        if (!errorExists) {
          stepFailure["errors"].push(fwjrError);
        };
      };

      stepFailure["input"] = stepFailure["input"].concat(workflowError["input"]);
      stepFailure["jobs"].push(jobID);
      for(var runNumber in workflowError["runs"]) {
        if (stepFailure["runs"].hasOwnProperty(runNumber)) {
          stepFailure["runs"][runNumber] = stepFailure["runs"][runNumber].concat(workflowError["runs"][runNumber]);
        } else {
          stepFailure["runs"][runNumber] = workflowError["runs"][runNumber];
        };
      };
    };
  };

  return workflowFailures;
}

function renderErrorDetails(errorInfo, stepDiv) {
  for(var errorIndex in errorInfo) {
    stepError = errorInfo[errorIndex];
    var errorDiv = document.createElement("div");

    if (errorIndex == 0) {
      errorDiv.style.margin = "0px 0px 0px 15px";
    } else {
      errorDiv.style.margin = "10px 0px 0px 15px";
    }

    errorDiv.innerHTML = "Error Type: " + stepError["type"] + "<br>";
    errorDiv.innerHTML += "Error Code: " + stepError["exitCode"] + "<br>";
    errorDiv.innerHTML += "Error Details:";
    stepDiv.appendChild(errorDiv);

    var errorDetailsDiv = document.createElement("div");
    errorDetailsDiv.style.margin = "0px 0px 0px 15px";
    errorDetailsDiv.innerHTML = stepError["details"];
    errorDiv.appendChild(errorDetailsDiv);
  };
};

function removeArrayDuplicates(someArray) {
  var uniqueValues = new Array;
  var prevValue = null;

  someArray.sort()
  for (var arrayIndex in someArray) {
    if (prevValue != someArray[arrayIndex]) {
      uniqueValues.push(someArray[arrayIndex]);
      prevValue = someArray[arrayIndex];
    };
  };

  return uniqueValues;
}

function renderJobDetails(jobIDs, stepDiv) {
  var uniqueJobIDs = removeArrayDuplicates(jobIDs) 

  var failedJobCountDiv = document.createElement("div");
  failedJobCountDiv.style.margin = "10px 0px 0px 15px";
  stepDiv.appendChild(failedJobCountDiv);

  if (uniqueJobIDs.length == 1) {
    failedJobCountDiv.innerHTML = "1 job failed ";
  } else {
    failedJobCountDiv.innerHTML = uniqueJobIDs.length + " jobs failed ";
  }

  if (jobIDs.length == 1) {
    failedJobCountDiv.innerHTML += "1 time:";
  } else {
    failedJobCountDiv.innerHTML += jobIDs.length + " times:";
  }
  
  var failedJobDiv = document.createElement("div");
  failedJobDiv.innerHTML = "";
  failedJobDiv.style.margin = "0px 0px 0px 15px";
  failedJobCountDiv.appendChild(failedJobDiv);
  var jobCount = 0;

  for (uniqueJobIndex in uniqueJobIDs) {
    failedJobDiv.innerHTML += "<a href=../../_show/jobSummary/" + uniqueJobIDs[uniqueJobIndex] + ">" + uniqueJobIDs[uniqueJobIndex] + "</a> ";
  };  
};

function renderInputDetails(inputLFNs, stepDiv) {
  var uniqueLFNs = removeArrayDuplicates(inputLFNs) 

  var failedInputCountDiv = document.createElement("div");
  failedInputCountDiv.style.margin = "10px 0px 0px 15px";
  stepDiv.appendChild(failedInputCountDiv);

  if (uniqueLFNs.length == 1) {
    failedInputCountDiv.innerHTML = "1 file was used as input for jobs with this error:";
  } else {
    failedInputCountDiv.innerHTML = uniqueLFNs.length + " files were used as input for jobs with this error:";
  }

  var failedInputDiv = document.createElement("div");
  failedInputDiv.innerHTML = "";
  failedInputDiv.style.margin = "0px 0px 0px 15px";
  failedInputCountDiv.appendChild(failedInputDiv);

  for (uniqueInputIndex in uniqueLFNs) {
    failedInputDiv.innerHTML += uniqueLFNs[uniqueInputIndex] + "<br>";
  };  
};

function renderRunLumiDetails(runLumiInfo, stepDiv) {
  var runLumiLabelDiv = document.createElement("div");
  runLumiLabelDiv.style.margin = "10px 0px 0px 15px";
  runLumiLabelDiv.innerHTML = "Run Information:";
  stepDiv.appendChild(runLumiLabelDiv);

  var runLumiDiv = document.createElement("div");
  runLumiStr = "";
  runLumiDiv.style.margin = "0px 0px 0px 15px";
  runLumiLabelDiv.appendChild(runLumiDiv);

  for (runNumber in runLumiInfo) {
    uniqueLumis = removeArrayDuplicates(runLumiInfo[runNumber]);
    runLumiStr += runNumber + ": ";
    for (lumiIndex in uniqueLumis) {
      runLumiStr += uniqueLumis[lumiIndex] + " ";
    };
    runLumiStr += "<br>";
  };  

  runLumiDiv.innerHTML = runLumiStr;
};

function renderWorkflowErrors(workflowName, errorDiv) {
  failedJobs = getFailedJobs(workflowName);
  workflowFailures = collateFailureInfo(failedJobs);

  for (var taskName in workflowFailures) {
    var taskDiv = document.createElement("div");
    taskDiv.style.margin = "0px 0px 0px 15px";
    taskDiv.innerHTML = taskName + ":";
    errorDiv.appendChild(taskDiv);

    for(var stepName in workflowFailures[taskName]) {
      var stepDiv = document.createElement("div");
      stepDiv.style.margin = "0px 0px 0px 15px";
      stepDiv.innerHTML = stepName + ":";
      taskDiv.appendChild(stepDiv);
      renderErrorDetails(workflowFailures[taskName][stepName]["errors"], stepDiv);
      renderJobDetails(workflowFailures[taskName][stepName]["jobs"], stepDiv);
      renderInputDetails(workflowFailures[taskName][stepName]["input"], stepDiv);
      renderRunLumiDetails(workflowFailures[taskName][stepName]["runs"], stepDiv);
    };
  };

  return;
};
