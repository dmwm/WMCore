function(doc) {
   if (doc.owner) {
        emit([doc.owner.name, doc.owner.group] , { '_id': doc._id, 'owner' : doc.owner} )
   }
}