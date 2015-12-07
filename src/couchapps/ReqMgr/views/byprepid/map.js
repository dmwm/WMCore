
function(doc) {
  if (doc.PrepID && doc.PrepID !== "None"){
      emit(doc.PrepID, null) ;
  }
  
  if (doc.TaskChain){
  	for (var i = 0; i < doc.TaskChain; i++) {
  		if (doc["Task" + (i+1)].PrepID && doc.PrepID !== "None") {
  			emit(doc["Task" + (i+1)].PrepID, null);
  		}
  	} 
  }
  
  if (doc.StepChain){
  	for (var i = 0; i < doc.StepChain; i++) {
  		if (doc["Step" + (i+1)].PrepID && doc.PrepID !== "None") {
  			emit(doc["Step" + (i+1)].PrepID, null);
  		}
  	} 
  }  
}
