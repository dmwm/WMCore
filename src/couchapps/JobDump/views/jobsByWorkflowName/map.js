function(doc) {
    emit([doc['workflow'], doc['jobid']],
            {'id': doc['_id'], 'rev': doc['_rev']});
}
