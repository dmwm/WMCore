function(doc) {
  if (doc.fileset){

       var i=0;
       for (var v in doc.fileset.files){
           i++;
       }
       emit([doc.fileset.collection_id], { "id": doc._id,"dataset": doc.fileset.dataset,   "files"  :i});
   }
}
