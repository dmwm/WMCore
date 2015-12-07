function(doc) {
  if (doc.Campaign){
     emit(doc.Campaign, null) ;
  } 
}