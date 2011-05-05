function(doc) {
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

    if(lastTransition['oldstate'] == 'jobfailed' &&
              lastTransition['newstate'] == 'jobcooloff') {
      emit([lastLocation, 'cooloff'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'complete' &&
               lastTransition['newstate'] == 'success') {
      emit([lastLocation, 'success'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'jobfailed' &&
               lastTransition['newstate'] == 'exhausted') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'submitfailed' &&
               lastTransition['newstate'] == 'exhausted') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'createfailed' &&
               lastTransition['newstate'] == 'exhausted') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'new' &&
               lastTransition['newstate'] == 'killed') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'created' &&
               lastTransition['newstate'] == 'killed') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'executing' &&
               lastTransition['newstate'] == 'killed') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'killed' &&
               lastTransition['newstate'] == 'killed') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'exhausted' &&
               lastTransition['newstate'] == 'cleanout') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'success' &&
               lastTransition['newstate'] == 'cleanout') {
      emit([lastLocation, 'success'], doc['jobid']);
    }
  }
}
