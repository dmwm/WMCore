function (doc, req) {
    if (doc === null) {
        log("Error: missing doc id - " + req.id);
        return [null, "ERROR: request not found - " + req.id];
    }
    var fields = JSON.parse(req.query.general_fields); 
    for (key in fields) {
        doc[key] = fields[key];
    }
    return [doc, 'OK']; 
} 
