
function(doc) {
  if (doc.OriginalRequestName){
      emit(doc.OriginalRequestName, doc.RequestName);
  }
}
