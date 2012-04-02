function(doc) {
  if (doc['type'] != 'job') {
    return;
  }

  var retryCount = 0;
  for (var transitionIndex in doc['states']) {
    var transition = doc['states'][transitionIndex];
    if (transition['oldstate'] != 'new' &&
        transition['newstate'] == 'created') {
      retryCount += 1;
    }

    if (transition.reported) {
      continue;
    } else if ((transition['oldstate'] == 'complete') &&
	       (transition['newstate'] == 'success')) {
      emit(doc['jobid'], {'index': transitionIndex,
                          'id': doc['jobid'],
                          'name': doc['name'],
                          'requestName': doc['workflow'],
                          'taskType': doc['taskType'],
                          'jobType': doc['jobType'],
                          'user': doc['user'],
                          'group': doc['group'],
                          'retryCount': retryCount,
                          'newState': transition['newstate'],
                          'oldState': transition['oldstate'],
                          'timestamp': transition['timestamp']});
    }
  }
}


