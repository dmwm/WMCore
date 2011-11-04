function(doc) {
    emit([doc['fwjr'].task.split('/')[1], doc['_id']],
            {'id': doc['_id'], 'rev': doc['_rev']});
}
