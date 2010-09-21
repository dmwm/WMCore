function(doc) {
  if (doc['type'] == 'state') {
    if (doc['oldstate'] == 'new' && doc['newstate'] == 'created') {
      emit([doc['workflow'], doc['task'], doc['jobid'], doc['timestamp']], 
            {'jobid': doc['jobid'], 'timestamp': doc['timestamp'], 
             'state': 'pending', 'task': doc['task']});
    } else if(doc['oldstate'] == 'created' && doc['newstate'] == 'executing') {
      emit([doc['workflow'], doc['task'], doc['jobid'], doc['timestamp']],
            {'jobid': doc['jobid'], 'timestamp': doc['timestamp'], 
             'state': 'running', 'task': doc['task']});
    } else if(doc['oldstate'] == 'jobfailed' && doc['newstate'] == 'jobcooloff') {
      emit([doc['workflow'], doc['task'], doc['jobid'], doc['timestamp']],
            {'jobid': doc['jobid'], 'timestamp': doc['timestamp'], 
             'state': 'cooloff', 'task': doc['task']});
    } else if(doc['oldstate'] == 'jobcooloff' && doc['newstate'] == 'created') {
      emit([doc['workflow'], doc['task'], doc['jobid'], doc['timestamp']],
            {'jobid': doc['jobid'], 'timestamp': doc['timestamp'], 
             'state': 'pending', 'task': doc['task']});
    } else if (doc['oldstate'] == 'complete' && doc['newstate'] == 'success') {
      emit([doc['workflow'], doc['task'], doc['jobid'], doc['timestamp']],
            {'jobid': doc['jobid'], 'timestamp': doc['timestamp'], 
             'state': 'success', 'task': doc['task']});
    } else if (doc['oldstate'] == 'jobfailed' && doc['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task'], doc['jobid'], doc['timestamp']],
            {'jobid': doc['jobid'], 'timestamp': doc['timestamp'], 
             'state': 'failure', 'task': doc['task']});
    } else if (doc['oldstate'] == 'submitfailed' && doc['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task'], doc['jobid'], doc['timestamp']],
            {'jobid': doc['jobid'], 'timestamp': doc['timestamp'], 
             'state': 'failure', 'task': doc['task']});
    } else if (doc['oldstate'] == 'createfailed' && doc['newstate'] == 'exhausted') {
      emit([doc['workflow'], doc['task'], doc['jobid'], doc['timestamp']],
            {'jobid': doc['jobid'], 'timestamp': doc['timestamp'], 
             'state': 'failure', 'task': doc['task']});
    }
  }
}
