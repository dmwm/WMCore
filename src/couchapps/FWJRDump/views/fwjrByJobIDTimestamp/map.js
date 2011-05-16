function(doc) {
       if (doc['type'] == 'fwjr') {
               emit([ doc['jobid'],doc['timestamp'] ], {'_id': doc['_id']});
       }
}
