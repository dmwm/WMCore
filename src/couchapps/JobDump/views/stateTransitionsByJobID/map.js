function(doc) {
  if (doc['type'] == 'state') {
    emit([doc['jobid'], doc['timestamp']], {'oldstate': doc['oldstate'],
                        'newstate': doc['newstate'],
			'location': doc['location'],
                        'timestamp': doc['timestamp']});
  }
}
