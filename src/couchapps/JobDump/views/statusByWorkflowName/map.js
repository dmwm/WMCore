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

    if (lastTransition['oldstate'] == 'new' && 
        lastTransition['newstate'] == 'created') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'pending', 'task': doc['task']});
    } else if(lastTransition['oldstate'] == 'created' && 
              lastTransition['newstate'] == 'executing') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'running', 'task': doc['task']});
    } else if(lastTransition['oldstate'] == 'jobfailed' &&
              lastTransition['newstate'] == 'jobcooloff') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'cooloff', 'task': doc['task']});
    } else if(lastTransition['oldstate'] == 'jobcooloff' &&
              lastTransition['newstate'] == 'created') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'pending', 'task': doc['task']});
    } else if (lastTransition['newstate'] == 'success') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'success', 'task': doc['task']});
    } else if (lastTransition['newstate'] == 'retrydone') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['newstate'] == 'killed') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['oldstate'] == 'exhausted' &&
               lastTransition['newstate'] == 'cleanout') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['oldstate'] == 'success' &&
               lastTransition['newstate'] == 'cleanout') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'success', 'task': doc['task']});
    } else {
       emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'transitioning', 'task': doc['task']});
    }
  }
}
