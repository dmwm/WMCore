
function(doc) {
  if (doc.InputDataset){
    emit(doc.InputDataset, null) ;
  } 
}