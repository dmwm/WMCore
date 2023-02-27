// update function
// input: valueKey (what to change), value - new value
function(doc, req)
{
   
   	if (doc === null) {
    	return [null, "Error: document not found"];
   	};
    /* Need to redefine the state transition comment out for now.
    var allowedTranistion = {
    	"new": ["Closed"],
    	"Closed": ["Merge", "AlcaSkim"],
    	"AlcaSkim": ["Merge"],
    	"Merge": ["Harvesting", "Processing Done"],
    	"Harvesting": ["Processing Done"],
    	"Processing Done": ["completed"],
    	"completed": ["normal-archived"],
    	"normal-archived": []
    };
    */
    var allowedStates = ["new", "Closed", "Merge", "AlcaSkim", "Harvesting",  
                         "Processing Done", "completed", "normal-archived"];
                         
    function isAllowedTransiton(oldStatus, newStatus) {
    	// This is tempro
    	if (oldStatus === "completed" && newStatus !== "normal-archived")  {
    		return false;
    	} else if  (oldStatus === "normal-archived"){
    		return false;
    	} else if  (oldStatus === newStatus){
    		return false;
    	} 
    	return true; 
    };
    
    function updateTransition(newStatus) {
    	
    	if (allowedStates.indexOf(newStatus) === -1) {
    		return "Not allowed state: '" + newStatus + "'";
    	}
    	if (doc.RequestStatus && !isAllowedTransiton(doc.RequestStatus, newStatus)) {
    		// don't update the status just ignore
    		return "Not allowed transition, from: '" + doc.RequestStatus + "' to '" + newStatus + "'";
    	}
    	doc.RequestStatus = newStatus;
    	
        var currentTS =  Math.round((new Date()).getTime() / 1000);
        var statusObj = {"Status": doc.RequestStatus, "UpdateTime": currentTS};
        
        if (!doc.RequestTransition) {
            doc.RequestTransition = new Array();
            doc.RequestTransition.push(statusObj);
        } else {
            doc.RequestTransition.push(statusObj);
        }
        return "OK";
    }
    // req.query is dictionary fields into the 
    // CMSCouch.Database.updateDocument() method, which is a dictionary
    var message = "OK";
    var newValues = req.query;
    for (key in newValues)
    {
        if (key == "RequestStatus") {
        	message = updateTransition(newValues[key]);
        } else if (key == "RequestTransition") {
    	    doc[key] = JSON.parse(newValues[key]);
    	} else {
    		doc[key] = newValues[key];
    	}

    }
    if (message == "OK") {
        return [doc, message];
    } else {
        // then do not change anything in the document
        return [null, message];
    }
}