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
        doc["status"] = reqDoc["status"];
        doc["down_components"] = reqDoc["down_components"];
        doc["acdc"] = reqDoc["acdc"];
        doc["timestamp"] = reqDoc["timestamp"];
    }
    return [doc, 'OK']; 
} 
