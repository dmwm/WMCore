function (doc, req) { 
    if (!doc.request_status) {
        doc.request_status = new Array()
    }
    //req.query.request_status is [status, timestamp], i.e. ['new', 112214411]
    doc.request_status.push(JSON.parse(req.query.request_status))
    return [doc, 'OK']; 
} 
