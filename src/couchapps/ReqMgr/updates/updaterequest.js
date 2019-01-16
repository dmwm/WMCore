// update function
// input: valueKey (what to change), value - new value
function(doc, req) {
    if (doc === null) {
    	return [null, "Error: document not found"];
    };
    
    function updateTransition(key, dn) {
        var keyAllowed = {"RequestStatus": "RequestTransition",
                          "RequestPriority": "PriorityTransition"};
        var keyMap = {"RequestStatus": "Status",
                      "RequestPriority": "Priority"};

        var transitionKey = keyAllowed[key]
        if (transitionKey === undefined) {
            return
        }

        var currentTS =  Math.round((new Date()).getTime() / 1000);
        var statusObj = {"UpdateTime": currentTS, "DN": dn};
        statusObj[keyMap[key]] = doc[key];

        if (!doc[transitionKey]) {
            doc[transitionKey] = new Array();
            doc[transitionKey].push(statusObj);
        } else {
            doc[transitionKey].push(statusObj);
        }
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
    var dn = null;

    var newValues = {};
    if (isEmpty(req.query)) {
        newValues = JSON.parse(req.body);
    } else {
        fromQuery = true;
        newValues = req.query;
    }

    // DN is not an allowed argument in StdBase, do not persist it
    if (newValues.hasOwnProperty("DN")) {
        dn = newValues.DN;
        delete newValues.DN;
     }

    for (key in newValues)
    {
    
        if (fromQuery) {
            if (key == "RequestTransition" ||
                key == "PriorityTransition" ||
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

        // Also update the transition dict if necessary
        updateTransition(key, dn);
        
        updateTaskStepChain("Task", key, newValues[key]);
        updateTaskStepChain("Step", key, newValues[key]);
    }
    return [doc, "OK"];
}
