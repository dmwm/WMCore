function(doc) {
  if (doc['type'] == 'state') {
    var output = {'pending': 0, 'running': 0, 'cooloff': 0, 'success': 0, 'failure': 0};

    if (doc['oldstate'] == 'new' && doc['newstate'] == 'created') {
      output['pending'] += 1;
    } else if(doc['oldstate'] == 'created' && doc['newstate'] == 'executing') {
      output['pending'] -= 1;
      output['running'] += 1;
    } else if(doc['oldstate'] == 'jobfailed' && doc['newstate'] == 'jobcooloff') {
      output['cooloff'] += 1;
    } else if(doc['oldstate'] == 'jobcooloff' && doc['newstate'] == 'created') {
      output['cooloff'] -= 1;
      output['pending'] += 1;
    } else if(doc['oldstate'] == 'executing' && doc['newstate'] == 'complete') {
      output['running'] -= 1;
    } else if (doc['oldstate'] == 'complete' && doc['newstate'] == 'success') {
      output['success'] += 1;
    } else if (doc['oldstate'] == 'jobfailed' && doc['newstate'] == 'exhausted') {
      output['failure'] += 1;
    }
   
    emit([doc['workflow'], doc['task'], doc['jobid']], output);
  }
}
