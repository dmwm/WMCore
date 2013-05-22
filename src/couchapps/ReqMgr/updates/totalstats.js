function (doc, req) {
    
    //TODO may need to added update flag to distinguish update and insert
    if (doc === null) {
        log("Error: missing doc id - " + req.id);
        return [null, "ERROR: request not found - " + req.id];
    }
    //request query is tightly coupled in WorkQueue.py _splitWork method
    var updateFlag = false; 
    if (!doc.TotalEstimatedJobs) {
        doc.TotalEstimatedJobs = Number(req.query.total_jobs);
    } else {
        doc.TotalEstimatedJobs += Number(req.query.total_jobs);
        updateFlag = true;
    }
    if (!doc.TotalInputEvents) {
        doc.TotalInputEvents = Number(req.query.input_events);
    } else {
        doc.TotalInputEvents += Number(req.query.input_events);
        updateFlag = true;
    }
    if (!doc.TotalInputLumis) {
        doc.TotalInputLumis = Number(req.query.input_lumis);
    } else {
        doc.TotalInputLumis += Number(req.query.input_lumis);
        updateFlag = true;
    }
    if (!doc.TotalInputFiles) {
        doc.TotalInputFiles = Number(req.query.input_num_files);
    } else {
        doc.TotalInputFiles += Number(req.query.input_num_files);
        updateFlag = true;
    }
    if (updateFlag) {
        return [doc, 'UPDATED'];
    } else {
        return [doc, 'INSERTED'];
    }
}