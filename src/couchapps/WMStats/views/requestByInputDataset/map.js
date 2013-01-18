
function(doc) {
  if (doc.inputdataset){
    emit(doc.inputdataset, null) ;
  } 
}