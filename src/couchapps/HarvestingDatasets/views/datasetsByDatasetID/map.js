function(doc) {
    if (doc['type'] == 'dataset') {
        emit(doc['datasetid'], {'_id': doc['_id']});
        }   
    }           

