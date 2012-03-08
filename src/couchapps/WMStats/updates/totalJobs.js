function (doc, req) { 
    if (!doc.total_jobs) {
        doc.total_jobs = req.query.total_jobs;
        return [doc, 'OK'];
    }
    return [null, 'EXIST'];
}