function(doc) {
    if (doc['type'] == 'fwjr' && doc["archivestatus"] && doc["archivestatus"] !== 'uploaded') {
    	var workflow = doc['fwjr'].task.split('/')[1];
        emit(workflow, null );
    }
}