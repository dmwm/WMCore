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

    if(lastTransition['newstate'] == 'jobcooloff') {
      emit([lastLocation, 'cooloff'], doc['jobid']);
    } else if (lastTransition['newstate'] == 'success') {
      emit([lastLocation, 'success'], doc['jobid']);
    } else if (lastTransition['newstate'] == 'retrydone') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['newstate'] == 'exhausted') {
      emit([lastLocation, 'failure'], doc['jobid']);
    } else if (lastTransition['newstate'] == 'killed') {
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
