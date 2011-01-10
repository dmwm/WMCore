function(doc, req){
    if (doc){
        log("have a doc for newuser...");
        return [ doc, "Exists"];
    }
    doc = {};
    var userName = "user-";
    userName += req.query['user']
    doc['_id'] = userName
    doc['user'] = {};
    doc.user['name'] = req.query['user'];
    doc.user['name'] = req.query['group'];
    doc.user['proxy'] = {};
    return [doc, "OK"];
}