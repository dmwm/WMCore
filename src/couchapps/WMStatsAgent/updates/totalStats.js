function (doc, req) {
    
    //TODO may need to added update flag to distinguish update and insert
    if (doc === null) {
        log("Error: missing doc id - " + req.id);
        return [null, "ERROR: request not found - " + req.id];
    }
    //request query is tightly coupled in WorkQueue.py _splitWork method
    var updateFlag = false; 
    if (!doc.total_jobs) {
        doc.total_jobs = Number(req.query.total_jobs);
    } else {
        doc.total_jobs += Number(req.query.total_jobs);
        updateFlag = true;
    }
    if (!doc.input_events) {
        doc.input_events = Number(req.query.input_events);
    } else {
        doc.input_events += Number(req.query.input_events);
        updateFlag = true;
    }
    if (!doc.input_lumis) {
        doc.input_lumis = Number(req.query.input_lumis);
    } else {
        doc.input_lumis += Number(req.query.input_lumis);
        updateFlag = true;
    }
    if (!doc.input_num_files) {
        doc.input_num_files = Number(req.query.input_num_files);
    } else {
        doc.input_num_files += Number(req.query.input_num_files);
        updateFlag = true;
    }
    if (updateFlag) {
        return [doc, 'UPDATED'];
    } else {
        return [doc, 'INSERTED'];
    }
}