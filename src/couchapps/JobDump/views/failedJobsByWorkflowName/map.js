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
    doc['states'].sort(stateSort);
    lastTransition = doc['states'].pop();

    if (lastTransition['oldstate'] == 'jobfailed' &&
        lastTransition['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task']], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'submitfailed' &&
               lastTransition['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task']], doc['jobid']);
    } else if (lastTransition['oldstate'] == 'createfailed' &&
               lastTransition['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task']], doc['jobid']);
    }
  }
}
