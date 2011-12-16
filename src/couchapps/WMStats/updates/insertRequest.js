function (doc, req) {
    // create doc if it is not exist. 
    if (!doc) {
        // _id needs to be specified
        if (!req.doc._id) {
            return [null, "Error"]
        }
        doc = req.doc
        return [doc, 'OK']   
    } else {
        return [null, "EXIST"]
    } 
} 
