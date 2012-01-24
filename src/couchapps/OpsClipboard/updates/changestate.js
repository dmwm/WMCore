// update function
// modify state of the request / document
function(doc, req)
{
    log(req);
    var newState = req.form["newState"];
    log(newState);
    if (newState != null)
    { 
        doc.state = newState;
        doc.timestamp = Date.now();
    }
    return [doc, "OK"]	
}