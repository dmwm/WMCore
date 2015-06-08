// update function
// input: valueKey (what to change), value - new value
function(doc, req) {
    if (doc === null) {
    	return [null, "Error: document not found"];
    };
    
    function updateTransition() {
        var currentTS =  Math.round((new Date()).getTime() / 1000);
        var dn = doc.DN || null;
        var statusObj = {"Status": doc.RequestStatus, "UpdateTime": currentTS, "DN": dn};
        
        if (!doc.RequestTransition) {
            doc.RequestTransition = new Array();
            doc.RequestTransition.push(statusObj);
        } else {
            doc.RequestTransition.push(statusObj);
        }
    }
    
    function updateTeams(team) {
        if (!doc.Teams) {
            doc.Teams = new Array();
            doc.Teams.push(team);
        } else {
            doc.Teams.push(team);
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
            key == "BlockWhitelist" ||
            key == "SoftwareVersions" ||
            key == "InputDatasetTypes" ||
            key == "InputDatasets" ||
            key == "OutputDatasets" ||
            key == "CustodialSites" ||
            key == "NoneCustodialSites" ||
            key == "AutoApproveSubscriptionSites" ||
            key == "Teams") {
    		
    		doc[key] = JSON.parse(newValues[key]);
    	} else if (key == "Team") {
    		updateTeams(newValues[key]);
    	} else {
    		doc[key] = newValues[key];
    	}
       
        if (key == "RequestStatus") {
        	updateTransition();
        }
    }
    return [doc, "OK"];
}
