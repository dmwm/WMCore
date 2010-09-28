function(doc) {
    if (doc['type'] == 'block') {
        emit(doc['datasetid'], {'_id': doc['_id']})
        }
    }

