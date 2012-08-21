function (doc, req) {
    if (doc === null) {
        log("Error: missing doc id - " + req.id);
        return [null, "ERROR: request not found - " + req.id];
    }
    //request query is tightly coupled in WorkQueue.py _splitWork method
    var updateFlag = false; 
    if (!doc.total_jobs) {
        doc.total_jobs = req.query.total_jobs;
        updateFlag = true;
    }
    if (!doc.input_events) {
        doc.input_events = req.query.input_events;
        updateFlag = true;
    }
    if (!doc.input_lumis) {
        doc.input_lumis = req.query.input_lumis;
        updateFlag = true;
    }
    if (!doc.input_num_files) {
        doc.input_num_files = req.query.input_num_files;
        updateFlag = true;
    }
    if (updateFlag) {
        return [doc, 'OK'];
    } else {
        return [null, 'EXIST'];
    }
}