WMStats.namespace("Requests");
WMStats.namespace("RequestsSummary");

WMStats.RequestsSummary = function() {
    //TODO add specific tier0 summary structure
    var tier0Summary = {};
    var requestSummary = new WMStats.GenericRequestsSummary(tier0Summary);
    return requestSummary;
};

WMStats.Requests = function(noFilterFlag) {
    var tier0Requests = new WMStats.GenericRequests(noFilterFlag);
    
    tier0Requests.getRequestAlerts = function() {
        
        var alertRequests = {};
        alertRequests['configError'] = [];
        alertRequests['siteError'] = [];
        alertRequests['paused'] = [];
        var ignoreStatus = ["closed-out",
                            "announced",
                            "aborted",
                            "rejected"]; 
        for (var workflow in this.getData()) {
            var requestInfo = this.getData(workflow);
                
            var reqSummary = this.getSummary(workflow);
            var cooloff = reqSummary.getTotalCooloff();
            var paused = reqSummary.getTotalPaused();
            var failure = reqSummary.getTotalFailure();
            var success = reqSummary.getJobStatus("success");
            var totalFailed = cooloff + paused + failure;
            if ( totalFailed > 0) {
                if (success === 0) {
                    alertRequests['configError'].push(this.getData(workflow));
                } else if ((totalFailed / (totalFailed + success)) > 0.85) {
                    alertRequests['siteError'].push(this.getData(workflow));
                }
            } else if (paused > 0){
            	alertRequests['paused'].push(this.getData(workflow));
            }
        }
        return alertRequests;
    };
    
    tier0Requests.numOfRequestError = function() {
    	/***
    	 * TODO: need to implement this
    	 ***/
    	var alertData = this.getRequestAlerts();
    	var numError = {};
    	numError.alert = 0;
    	for (var error in alertData) {
        	numError.alert += alertData[error].length;
        };
        
        return numError;
    };
    return tier0Requests;
};
