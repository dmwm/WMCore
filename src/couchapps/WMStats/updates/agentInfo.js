function (doc, req) {
    // create doc if it is not exist.
    // If exists, replace existing doc
    var agentInfo = req.query || JSON.parse(req.body);

    if (!doc) {
        doc = agentInfo;
        if (!doc._id) {
            return [null, "Error"];
        }
    } else {
        for (var prop in agentInfo) {
            doc[prop] = agentInfo[prop];
        }
    }
    return [doc, 'OK'];
}
