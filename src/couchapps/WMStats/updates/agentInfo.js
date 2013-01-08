function (doc, req) {
    // create doc if it is not exist.
    // If exists, replace existing doc
    reqDoc = JSON.parse(req.query.agent_info)
    if (!doc) { 
        doc = reqDoc;
        if (!doc._id) {
            return [null, "Error"];
        }
    } else {
        for (var prop in reqDoc) {
            doc[prop] = reqDoc[prop];
        }
    }
    return [doc, 'OK']; 
} 
