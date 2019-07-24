function(doc) {
  if (doc.ConfigType){
     emit(doc.ConfigType, null) ;
  }
}