/**
 * Return documents id and version indexed by collection name.
 * @author Diego Ballesteros
 */
function(doc) {
    if (doc.collection_name) {
        emit(doc.collection_name, {"_rev" : doc._rev, "_id" : doc._id});
    }
}
