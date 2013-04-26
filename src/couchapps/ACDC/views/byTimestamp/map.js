/**
 * Return documents id and version indexed by timestamp,
 * this assume that user records have no timestamps.
 * @author Diego Ballesteros
 */
function(doc) {
    if (doc.timestamp) {
        emit(doc.timestamp, {"_rev" : doc._rev, "_id" : doc._id});
    }
}
