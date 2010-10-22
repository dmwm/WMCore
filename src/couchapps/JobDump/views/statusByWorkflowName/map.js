function(doc) {
  log(doc);

  if (doc['type'] == 'job') {
    lastTransition = doc['states'].pop();

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
    } else if (lastTransition['oldstate'] == 'complete' &&
               lastTransition['newstate'] == 'success') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'success', 'task': doc['task']});
    } else if (lastTransition['oldstate'] == 'jobfailed' &&
               lastTransition['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['oldstate'] == 'submitfailed' &&
               lastTransition['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['oldstate'] == 'createfailed' &&
               lastTransition['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['oldstate'] == 'new' &&
               lastTransition['newstate'] == 'killed') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['oldstate'] == 'created' &&
               lastTransition['newstate'] == 'killed') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['oldstate'] == 'executing' &&
               lastTransition['newstate'] == 'killed') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    } else if (lastTransition['oldstate'] == 'killed' &&
               lastTransition['newstate'] == 'killed') {
      emit([doc['workflow'], doc['task'], doc['jobid']],
            {'jobid': doc['jobid'], 'state': 'failure', 'task': doc['task']});
    }
  }
}
