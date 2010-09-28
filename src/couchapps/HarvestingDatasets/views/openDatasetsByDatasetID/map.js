function(doc) {
    if (doc['type'] == 'dataset' & doc['status'] == 'open' ) {
        emit(doc['datasetid'], {'_id': doc['_id']});
        }
    }

