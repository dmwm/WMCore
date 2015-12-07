function(doc) {
  if (doc.DataPileup && doc.DataPileup !== "None"){
      emit(doc.DataPileup, null) ;
  }
  
  if (doc.TaskChain){
  	for (var i = 0; i < doc.TaskChain; i++) {
  		if (doc["Task" + (i+1)].DataPileup && doc.DataPileup !== "None") {
  			emit(doc["Task" + (i+1)].DataPileup, null);
  		}
  	} 
  }
  
  if (doc.StepChain){
  	for (var i = 0; i < doc.StepChain; i++) {
  		if (doc["Step" + (i+1)].DataPileup && doc.DataPileup !== "None") {
  			emit(doc["Step" + (i+1)].DataPileup, null);
  		}
  	} 
  }  
}