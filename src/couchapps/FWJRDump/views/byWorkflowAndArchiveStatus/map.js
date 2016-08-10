function(doc) {
    if (doc['type'] == 'fwjr' && doc["archivestatus"]) {
    	var workflow = doc['fwjr'].task.split('/')[1];
    	emit([workflow, doc["archivestatus"]] , null );
    }
}