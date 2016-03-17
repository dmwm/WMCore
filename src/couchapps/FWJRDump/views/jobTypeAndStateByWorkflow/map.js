function(doc) {
    if (doc['type'] == 'fwjr' && doc["jobtype"] && doc["jobstate"]) {
        var workflow = doc['fwjr'].task.split('/')[1];
        emit([workflow, doc["jobtype"], doc["jobstate"]], null );
    }
}
