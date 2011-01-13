function getParentJobForFile(requestName, fileLFN) {
  // Retrieve the ID of the job that produce this file.  If no parent job
  // exists, return null.
  xmlhttp = new XMLHttpRequest();
  xmlhttp.open("GET", "../../_view/jobsByOutputLFN?stale=ok&startkey=[\"" + requestName + "\",\"" + fileLFN + "\"]&endkey=[\"" + requestName + "\",\"" + fileLFN + "\",{}]", false);
  xmlhttp.send();

  var results = eval("(" + xmlhttp.responseText + ")")["rows"];
  if (results.length > 0) {
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

function renderJobInputFiles(requestName, inputFiles, jobID, inputDiv) {
  // Render information about the input to this job, the jobs that produced the
  // input and any of jobs that used the same input file.
  var inputContent = "";

  for (var inputFileIndex in inputFiles) {
    inputContent += "  " + inputFiles[inputFileIndex] + "\n";
    var parentJob = getParentJobForFile(requestName, inputFiles[inputFileIndex]);

    if (parentJob) {
      inputContent += "    Parent: <a href=\"../jobSummary/" + parentJob + "\">" + parentJob + "</a>";
    };

    var siblingJobs = getSiblingJobsForFile(requestName, jobID, inputFiles[inputFileIndex]);
    if (siblingJobs.length > 0) {
      if (siblingJobs.length == 1) {
        inputContent += " Sibling: ";
      } else {
        inputContent += " Siblings: ";
      }

      for (var siblingJobIndex in siblingJobs) {
        inputContent += "<a href=\"../jobSummary/" + siblingJobs[siblingJobIndex] + "\">" + siblingJobs[siblingJobIndex] + "</a>";
        if (siblingJobIndex == siblingJobs.length - 1) {
          inputContent += "\n";
        } else {
          inputContent += ", ";
        };
      };
    };
  };

  inputDiv.innerHTML = inputContent;
};
