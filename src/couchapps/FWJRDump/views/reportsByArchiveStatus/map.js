function(doc) {
    if (doc['type'] == 'fwjr' && doc["archivestatus"]) {
        emit(doc["archivestatus"], null);
    }
}
