WMStats.namespace("Requests");
WMStats.namespace("RequestsSummary");

WMStats.RequestsSummary = function() {
    //TODO add specific tier0 summary structure
    var tier0Summary = {};
    var requestSummary = new WMStats.GenericRequestsSummary(tier0Summary);
    return requestSummary;
};

WMStats.Requests = function(data) {
    var tier0Requests = new WMStats.GenericRequests(data);
    
    tier0Requests.getRequestAlerts = function() {
        
        var alertRequests = {};
        alertRequests['configError'] = [];
        alertRequests['siteError'] = [];
        alertRequests['cPaused'] = [];
        alertRequests['sPaused'] = [];
        alertRequests['jPaused'] = [];
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
            var sPaused = reqSummary.getJobStatus("paused.submit");
            var jPaused = reqSummary.getJobStatus("paused.job");
            var cPaused = reqSummary.getJobStatus("paused.create");
            var totalFailed = failure;
            if (cPaused > 0){
            	alertRequests['cPaused'].push(this.getData(workflow));
            } else if (sPaused > 0){
            	alertRequests['sPaused'].push(this.getData(workflow));
            } else if (jPaused > 0){
            	alertRequests['jPaused'].push(this.getData(workflow));
            }
            /* 
            else if ( totalFailed > 0) {
                if (success === 0) {
                    alertRequests['configError'].push(this.getData(workflow));
                } else if ((totalFailed / (totalFailed + success)) > 0.85) {
                    alertRequests['siteError'].push(this.getData(workflow));
                }
            }*/
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
