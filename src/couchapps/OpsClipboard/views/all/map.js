function(doc){
    // show all docs by state
    emit(doc['state'], doc._id);
}