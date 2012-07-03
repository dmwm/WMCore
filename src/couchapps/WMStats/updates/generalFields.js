function (doc, req) {
    var fields = JSON.parse(req.query.general_fields); 
    for (key in fields) {
        doc[key] = fields[key];
    }
    return [doc, 'OK']; 
} 
