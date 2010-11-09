function(doc) {
   if (doc.owner) {
        emit([doc.owner.user, doc.owner.group] , { '_id': doc._id, 'owner' : doc.owner} )
   }
}