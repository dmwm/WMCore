function(doc, req) {
  if (doc._deleted || doc.type === 'agent_request'){
    return false;
  } else {
  	return !doc._id.match('_design/(.*)');
  }
}