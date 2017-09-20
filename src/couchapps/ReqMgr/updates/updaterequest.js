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
        doc.Teams = new Array();
        doc.Teams.push(team);
    }

    // req.query is dictionary fields into the 
    // CMSCouch.Database.updateDocument() method, which is a dictionary
    function isEmpty(obj) {
        for(var prop in obj) {
            if(obj.hasOwnProperty(prop))
                return false;
        }
        return true;
    }

    function updateTaskStepChain(chainType, prop, value) {
        if (doc[chainType + "Chain"]) {
            var numChain = doc[chainType + "Chain"];
            for (var i=1; i <= numChain; i++) {
                if (doc[chainType + i] && doc[chainType + i][prop] && 
                    value.hasOwnProperty(doc[chainType + i][chainType + "Name"])) {
                    
                    doc[chainType + i][prop] = value[doc[chainType + i][chainType + "Name"]];
                }
            }
        }
    }

    // req.query is dictionary fields into the 
    // CMSCouch.Database.updateDocument() method, which is a dictionary
 
    //TODO: only accepts request body for the argument
    var fromQuery = false;
    
    var newValues = {};
    if (isEmpty(req.query)) {
        newValues = JSON.parse(req.body);
    } else {
        fromQuery = true;
        newValues = req.query;
    }
    
    for (key in newValues)
    {
    
        if (fromQuery) {
            if (key == "RequestTransition" ||
                key == "SiteWhitelist" ||
                key == "SiteBlacklist" ||
                key == "BlockWhitelist" ||
                key == "CMSSWVersion" ||
                key == "InputDataset" ||
                key == "OutputDatasets" ||
                key == "CustodialSites" ||
                key == "NonCustodialSites" ||
                key == "AutoApproveSubscriptionSites" ||
                key == "OutputModulesLFNBases") {

               doc[key] = JSON.parse(newValues[key]);
           }
        }

        doc[key] = newValues[key];

        // FIXME keep updating Teams for now to make it easier to deprecate in HG1710
        if (key == "Team") {
            updateTeams(newValues[key]);
        }

        // If key is RequestStatus, also update the transition
        if (key == "RequestStatus") {
            updateTransition();
        }
        
        updateTaskStepChain("Task", key, newValues[key]);
        updateTaskStepChain("Step", key, newValues[key]);
    }
    return [doc, "OK"];
}
