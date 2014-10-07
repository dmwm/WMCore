WMStats.namespace("RunSummary");
WMStats.namespace("RunStatus");

/*
 * run status is ordered as lower index determines combined status.
 * i.e. If one request is "Active" and the other is "Real Time Processing"
 * run status will be Active 
 * There is no way to determine given run has PromptReco "Real Timp Done" is moved
 * to right before Complete status. If run contains only Express and Repack workflows,
 * it will never move to Complete run status. However workflow status will be moved to 
 * completed by task archiver and eventually archived.
 */
WMStats.RunStatus = ["Active", "Real Time Processing", "Real Time Merge", 
                     "Real Time Harvesting", "PromptReco", "Reco Merge",
                     "Reco Harvest", "Processing Done", "Real Time Done","Complete"];
                     
WMStats.RunSummary = function() {
    var runSummary = {numRequests: 0};
    var runSummary = new WMStats.GenericRequestsSummary(runSummary);
    
    function getRunStatus(doc) {
        var wfStatus = doc["request_status"][doc["request_status"].length - 1].status;
        var wfPrefix = doc['workflow'].split("_")[0].toLowerCase();
        if (wfPrefix == "express" || wfPrefix == "repack") {
            if (wfStatus == "new") {
                return WMStats.RunStatus[0];
            } else if (wfStatus == "Closed") {
                return WMStats.RunStatus[1];
            } else if (wfStatus == "Merge") {
                return WMStats.RunStatus[2];
            } else if (wfStatus == "Harvesting") {
                return WMStats.RunStatus[3];
            } else if (wfStatus == "Processing Done") {
                return WMStats.RunStatus[8];
            }
        } else if (wfPrefix == "promptreco") {
            if (wfStatus == "new" || wfStatus == "Closed" || wfStatus == "AlcaSkim" ) {
                return WMStats.RunStatus[4];
            } else if (wfStatus == "Merge") {
                return WMStats.RunStatus[5];
            } else if (wfStatus == "Harvesting") {
                return WMStats.RunStatus[6];
            } else if (wfStatus == "Processing Done") {
                return WMStats.RunStatus[7];
            } 
        }
        return WMStats.RunStatus[9];
    }
    
    runSummary.summaryStructUpdateFuction = function(baseObj, additionObj, field) {
        if (field === "runStatus") {
            if (WMStats.RunStatus.indexOf(baseObj[field]) > WMStats.RunStatus.indexOf(additionObj[field])) {
                baseObj[field] = additionObj[field];
            }
        } else {
            baseObj[field] += additionObj[field];
        }
    };

    runSummary.createSummaryFromRequestDoc = function(doc) {
        var summary = WMStats.RunSummary();
        summary.summaryStruct.numRequests = 1;
        summary.summaryStruct.runStatus = getRunStatus(doc);
        summary.jobStatus = this._get(doc, 'status', {});
        
        return summary;
    };
    
    return runSummary;
};
