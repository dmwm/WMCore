// update function
// input: valueKey (what to change), value - new value
function(doc, req)
{
    log(req);

    function updateTransition() {
        var currentTS =  Math.round((new Date()).getTime() / 1000);
        var statusObj = {"Status": doc.RequestStatus, "UpdateTime": currentTS};
        
        if (!doc.RequestTransition) {
            doc.RequestTransition = new Array();
            doc.RequestTransition.push(statusObj);
        } else {
            doc.RequestTransition.push(statusObj);
        }
    }
    // req.query is dictionary fields into the 
    // CMSCouch.Database.updateDocument() method, which is a dictionary
    var newValues = req.query;
    for (key in newValues)
    {   
        if (key == "RequestTransition" ||
            key == "SiteWhitelist" ||
            key == "SiteBlacklist" ||
            key == "Teams") {
    		
    		doc[key] = JSON.parse(newValues[key]);
    	} else {
    		doc[key] = newValues[key];
    	}
       
        if (key == "RequestStatus") {
        	updateTransition();
        }
    }
    return [doc, "OK"];
}