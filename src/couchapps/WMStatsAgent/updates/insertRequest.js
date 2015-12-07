function (doc, req) {
    // create doc if it is not exist. 
    if (!doc) {
        // _id needs to be specified
        doc = JSON.parse(req.query.doc);
        if (!doc._id) {
            return [null, "Error"];
        }
        return [doc, 'OK'];   
    } else {
        return [null, "EXIST"];
    } 
} 
