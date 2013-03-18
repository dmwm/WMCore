// update function
// input: valueKey (what to change), value - new value
function(doc, req)
{
	log(req);
	// req.query is dictionary fields into the 
	// CMSCouch.Database.updateDocument() method, which is a dictionary
	var newValues = req.query;
	for (key in newValues)
	{
		doc[key] = newValues[key];  
	}
    return [doc, "OK"]	
}