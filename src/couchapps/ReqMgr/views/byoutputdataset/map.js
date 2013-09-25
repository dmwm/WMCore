function(doc) {
  if (doc.OutputDatasets){
    for (var i in doc.OutputDatasets){
        emit(doc.OutputDatasets[i], null);
    }
  } 
}
