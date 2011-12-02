// update function
// adding description to the request document by an Operator
function(doc, req)
{
	var newDesc = req.form["newDescription"];
    log(newDesc);
    // doc.description[Date.now()] = newDesc;
    // the above has problem since from Python JSON complains to have
    // timestamp int as keys, so description request entry is made
    // a list of dictionaries which should allow time ordering
    var descEntry = {"timestamp": Date.now(), "info": newDesc};
    doc.description.push(descEntry);
    return [doc, "OK"]
}