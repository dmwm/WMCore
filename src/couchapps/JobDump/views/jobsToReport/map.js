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
    } else if ((transition['oldstate'] == 'executing' &&
                transition['newstate'] == 'jobfailed') ||
               (transition['oldstate'] == 'complete') ||
               (transition['oldstate'] == 'created' &&
                transition['newstate'] == 'executing')) {
      emit(doc['jobid'], {'index': transitionIndex,
                          'id': doc['jobid'],
                          'name': doc['name'],
                          'requestName': doc['workflow'],
                          'taskType': doc['taskType'],
                          'user': doc['user'],
                          'group': doc['group'],
                          'retryCount': retryCount,
                          'newState': transition['newstate'],
                          'oldState': transition['oldstate']});
    }
  }
}


