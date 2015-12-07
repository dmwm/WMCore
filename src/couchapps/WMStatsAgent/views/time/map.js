function(doc) {
    // this is used for document deletion
    if (doc.timestamp && (doc.type == "agent_request" || doc.type == 'agent')) {
        emit(doc.timestamp, {'id': doc['_id'], 'rev': doc['_rev']});
    }
}