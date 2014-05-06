function (doc, req) {
	
	// create doc if it is not exist. 
    if (doc === null) {
        // _id needs to be specified
        var newDoc = JSON.parse(req.body);
        if (newDoc._id) {
            //TODO: add the validation
        	doc = newDoc;
        	return [doc, 'OK: inserted'];
        } else{
        	return [null, "Error: _id need to be specified " + newDoc];
        };   
    } else {
        var fields = JSON.parse(req.body); 
    	for (var key in fields) {
    		if (key !== "_id") {
	        	doc[key] = fields[key];
	       	}
	    }
	    return [doc, 'OK: updated'];
    };  
}; 
