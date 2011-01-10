function(doc, req){
    if (doc){
        log("have a doc for newgroup...");
        return [ doc, "Exists"];
    }
    
    
        
    doc = {};
    var groupName = "group-";
    groupName += req.query['group']
    doc['_id'] = groupName
    doc['group'] = {};
    doc.group['name'] = req.query['group'];
    doc.group['administrators'] = {};
    doc.group['associated_sites'] = {};
    return [doc, "OK"];
}