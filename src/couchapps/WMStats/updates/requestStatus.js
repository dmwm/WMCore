function (doc, req) { 
    if (!doc.request_status) {
        doc.request_status = new Array()
    }
    //req.status is [status, timestamp], i.e. ['new', 112214411]
    doc.request_status.push(req.status)
    return [doc, 'OK']; 
} 
