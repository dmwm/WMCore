function(doc, req) {
    if (doc._deleted){
       return false;
    }
    return true;
}