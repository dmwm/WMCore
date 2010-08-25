function(doc) {
  if (doc.fileset){
       var i=0;
       for (var v in doc.fileset.files){
           i++;
       }
       emit([doc._id], i);
   }
}