function (doc, req) {
    // create doc if it is not exist.
    // If exists, replace existing doc
    var agentInfo = req.query.agent_info || JSON.parse(req.body).agent_info;
    var reqDoc = JSON.parse(agentInfo);
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
