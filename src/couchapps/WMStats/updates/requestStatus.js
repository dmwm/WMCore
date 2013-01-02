function (doc, req) { 
    var statusObj = JSON.parse(req.query.request_status);
    if (doc === null) {
        log("Error: missing doc id - " + req.id);
        return [null, "ERROR: request not found - " + req.id];
    }
    if (!doc.request_status) {
        doc.request_status = new Array();
        doc.request_status.push(statusObj);
        return [doc, 'OK']; 
    } else {
        // only update when status changed
        var lastState = doc.request_status[doc.request_status.length - 1].status;
        var legalTransition = true;
        if (lastState != statusObj.status) {
            if (lastState == "completed") {
                if ((statusObj.status != "closed-out") && (statusObj.status != "normal-archived")) {
                    legalTransition = false;
                }
            } else if ((lastState == "aborted-completed") && (statusObj.status != "abort-archived")) {
                legalTransition = false;
            } else if ((lastState == "normal-archived") || (lastState == "abort-archived")) {
                legalTransition = false;
            }
            if (legalTransition) {
                doc.request_status.push(statusObj);
                return [doc, 'OK'];
            } else {
                return [null, "ILLEGAL TRANSITION"];
            }
        }
        return [null, "SAME STATE"];
    } 
} 
