function (doc, req) { 
    statusObj = JSON.parse(req.query.request_status);
    if (!doc.request_status) {
        doc.request_status = new Array();
        doc.request_status.push(statusObj);
        return [doc, 'OK']; 
    } else {
        // only update when status changed
        if (doc.request_status[doc.request_status.length - 1].status != statusObj.status) {
            doc.request_status.push(statusObj);
            return [doc, 'OK'];
        }
        return [null, "SAME STATE"];
    } 
} 
