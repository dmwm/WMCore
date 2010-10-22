function(doc) {
  if (doc['type'] == 'job') {
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
