function(doc) {
  /* this is used for WMBSService and OLD GlobalMonitor.
   * need to be deplicated
   */
  function stateSort(a, b) {
    if (a['timestamp'] > b['timestamp']) {
      return 1;
    } else if (a['timestamp'] == b['timestamp']) {
      return 0;
    }

  return -1;
  }

  if (doc['type'] == 'job') {
    var stateList = new Array();
    for (var transitionIndex in doc['states']) {
      stateList.push(doc['states'][transitionIndex]);
    }
    
    stateList.sort(stateSort);
    lastTransition = stateList.pop();

    var lastLocation = null;
    for (stateIndex in stateList) {
      if (stateList[stateIndex]["newstate"] == "executing") {
        lastLocation = stateList[stateIndex]["location"];
      }
    }

    var lastHour = lastTransition['timestamp'] - (lastTransition['timestamp'] % 3600);
    
    // cooloff state
    if (lastTransition['newstate'] == 'jobcooloff') {
      emit([lastHour, lastLocation, 'cooloff'], doc['jobid']);
    } 
    // success state
    else if (lastTransition['newstate'] == 'success') {
      emit([lastHour, lastLocation, 'success'], doc['jobid']);
	} else if (lastTransition['oldstate'] == 'success' &&
               lastTransition['newstate'] == 'cleanout') {
      emit([lastHour, lastLocation, 'success'], doc['jobid']);
    } 
    // failure state
    else if (lastTransition['newstate'] == 'exhausted') {
      emit([lastHour, lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'exhausted' &&
               lastTransition['newstate'] == 'cleanout') {
      emit([lastHour, lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['newstate'] == 'retrydone') {
      emit([lastHour, lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['newstate'] == 'killed') {
      emit([lastHour, lastLocation, 'failure'], doc['jobid']);
    }
  }
}
