
function(doc) {
  if (doc.prep_id){
      emit(doc.prep_id, null) ;
  } 
}
