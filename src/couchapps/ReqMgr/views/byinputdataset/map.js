function(doc) {
  if (doc.InputDataset && doc.InputDataset !== "None"){
      emit(doc.InputDataset, null) ;
  }
  
  if (doc.TaskChain){
  	for (var i = 0; i < doc.TaskChain; i++) {
  		if (doc["Task" + (i+1)].InputDataset && doc["Task" + (i+1)].InputDataset !== "None") {
  			emit(doc["Task" + (i+1)].InputDataset, null);
  		}
  	} 
  }
  
  if (doc.StepChain){
  	for (var i = 0; i < doc.StepChain; i++) {
  		if (doc["Step" + (i+1)].InputDataset && doc["Step" + (i+1)].InputDataset !== "None") {
  			emit(doc["Step" + (i+1)].InputDataset, null);
  		}
  	} 
  }  
}
