function(doc) {
  if (doc.outputdatasets){
    for (var i in doc.outputdatasets){
        emit(doc.outputdatasets[i], null);
    }
  } 
}
