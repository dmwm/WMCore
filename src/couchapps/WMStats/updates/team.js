function (doc, req) { 
    if (!doc.teams) {
        doc.teams = new Array()
    }
    doc.teams.push(req.query.team)
    return [doc, 'OK']; 
} 
