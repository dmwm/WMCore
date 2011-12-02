// update function
// modify state of the request / document
// TODO
// here should be validation of req.form["newState"] against allowed states
// and allowed transitions according to requeststatus.js
function(doc, req)
{
    log(req);
    var newState = req.form["newState"];
    log(newState);
    if ( newState != null )
    { 
        doc.state = newState;
        doc.timestamp = Date.now();
    }
    return [doc, "OK"]	
}