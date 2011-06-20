function(doc, req){
    
    //doc.state = req.query.state;
    log(req);
    var newState = req.form["selectState"];
    log(newState);
    if ( newState != null ){ 
        doc.state = newState;
        doc.timestamp = Date.now();
    }
    return [doc, "OK"]
    
}