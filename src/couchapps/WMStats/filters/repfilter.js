function(doc, req) {
  if (doc._deleted){
    return false;
  } else {
  	return !doc._id.match('_design/(.*)');
  }  
}