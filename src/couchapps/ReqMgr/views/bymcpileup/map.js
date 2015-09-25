function(doc) {
  if (doc.MCPileup && doc.MCPileup !== "None"){
      emit(doc.MCPileup, null) ;
  }
  
  if (doc.TaskChain){
  	for (var i = 0; i < doc.TaskChain; i++) {
  		if (doc["Task" + (i+1)].MCPileup && doc.MCPileup !== "None") {
  			emit(doc["Task" + (i+1)].MCPileup, null);
  		}
  	} 
  }
  
  if (doc.StepChain){
  	for (var i = 0; i < doc.StepChain; i++) {
  		if (doc["Step" + (i+1)].MCPileup && doc.MCPileup !== "None") {
  			emit(doc["Step" + (i+1)].MCPileup, null);
  		}
  	} 
  }  
}