function (doc, req) {
	/*
	 * function replace request parameters, 
	 * Need to make sure replacement has same format not just string
	 * use post for complex format
	 * WARNING: use with caution this is irreversable
	 */
    if (doc === null) {
        log("Error: missing doc id - " + req.id);
        return [null, "ERROR: request not found - " + req.id];
    };
    var fields = JSON.parse(req.body);
    for (key in fields) {
        doc[key] = fields[key];
    };
    return [doc, 'OK']; 
}; 