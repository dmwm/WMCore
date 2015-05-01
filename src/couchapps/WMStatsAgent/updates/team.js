function (doc, req) { 
    if (doc === null) {
        log("Error: missing doc id - " + req.id);
        return [null, "ERROR: request not found - " + req.id];
    }
    if (!doc.teams) {
        doc.teams = new Array()
    }
    doc.teams.push(req.query.team)
    return [doc, 'OK']; 
} 
