WMStats.namespace("Requests");
WMStats.namespace("RequestsSummary");

WMStats.RequestsSummary = function() {
    var tier1Summary = {totalEvents: 0,
                        processedEvents: 0};
    var requestSummary = new WMStats.GenericRequestsSummary(tier1Summary);

    requestSummary.createSummaryFromRequestDoc = function(doc) {
        var summary = WMStats.RequestsSummary();
        summary.summaryStruct.totalEvents = Number(this._get(doc, "input_events", 0));
        summary.summaryStruct.processedEvents = this._get(doc, "output_progress.0.events", 0);
        summary.summaryStruct.progress = this.getAvgProgressSummary(doc);
        summary.summaryStruct.length = 1;
        summary.jobStatus = this._get(doc, 'status', {});
        //support legacy code which had cooloff jobs instead cooloff.create, cooloff.submit
        //cooloff.job
        if ((typeof summary.jobStatus.cooloff) === "number") {
            summary.jobStatus.cooloff = {create: 0, submit: 0, job: summary.jobStatus.cooloff};
        }
        return summary;
    };

    return requestSummary;
};

WMStats.Requests = function(data) {
    var tier1Requests = new WMStats.GenericRequests(data);

    tier1Requests.estimateCompletionTime = function(request) {
        //TODO need to improve the algo
        // no infomation to calulate the estimate completion time
        var aData = WMStats.ActiveRequestModel.getData().getData(request);
        var reqSummary = this.getSummary(request);
        var completedJobs = reqSummary.getJobStatus("success") + reqSummary.getTotalFailure();
        if (completedJobs == 0) return -1;
        // get running start time.
        var requestStatus = this._get(aData, "request_status");
        var lastStatus = requestStatus[requestStatus.length - 1];
        
        //request is done
        if (lastStatus.status !== 'running' &&
            lastStatus.status !== 'running-closed' &&
            lastStatus.status !== 'running-open' &&
            lastStatus.status !== 'force-complete') return 0;

        var totalJobs = reqSummary.getWMBSTotalJobs() - reqSummary.getJobStatus("canceled");
        // jobCompletion percentage 
        var completionRatio = (completedJobs / totalJobs);
        var queueInjectionRatio = reqSummary.getJobStatus("inWMBS") / this._get(aData, 'total_jobs', 1);
        var duration = Math.round(Date.now() / 1000) - lastStatus.update_time;
        var timeLeft = Math.round((duration / (completionRatio * queueInjectionRatio)) - duration);
        
        return timeLeft;
    };
    
    tier1Requests.getRequestAlerts = function() {
        
        var alertRequests = {};
        alertRequests['configError'] = [];
        alertRequests['siteError'] = [];
        alertRequests['failed'] = [];
        var ignoreStatus = ["closed-out",
                            "announced",
                            "aborted",
                            "rejected"]; 
        for (var workflow in this.getData()) {
            var requestInfo = this.getData(workflow);
            // filter ignoreStatus
            lastState = requestInfo.getLastState();
            if (ignoreStatus.indexOf(lastState) !== -1) continue;
            if (lastState == "failed" || lastState == "epic-FAILED") {
                alertRequests['failed'].push(this.getData(workflow));
            };
                
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
            }
        }
        return alertRequests;
    };
    
    
    tier1Requests.requestNotPulledAlert = function() {
        var alertRequests = {};
        alertRequests['assignedStall'] = [];
        alertRequests['statusStall'] = [];
        for (var workflow in this.getData()) {
            var reqStatusInfo = this.getRequestStatusAndTime(workflow);
            var currentTime = Math.round(new Date().getTime() / 1000);
            var assignThreshold = 7200; // 2 hours
            var status = reqStatusInfo.status;
            //Global Queue not pulling case
            if (status == "assigned") {
                if ((currentTime - reqStatusInfo.update_time) > assignThreshold) {
                    alertRequests['assignedStall'].push(this.getData(workflow));
                }
            };
            //TODO: this needs to be redefined for several use case
            // since local queue is partially pulled check
            //localqueue not pulled case
            var twoDayThreshold = 3600 * 24 * 2; //2 days
            var runningJobs = this.getSummary(workflow).getRunning();
            if (runningJobs < 1 && (status == "acquired" || status == "running-open" || 
                status == "running-closed")) {
                if ((currentTime - reqStatusInfo.update_time) > twoDayThreshold) {
                    alertRequests['statusStall'].push(this.getData(workflow));
                };
            };
        };
        return alertRequests;
    };

    tier1Requests.numOfRequestError = function() {
    	var alertData = this.getRequestAlerts();
    	var numError = {};
    	numError.alert = 0;
    	for (var error in alertData) {
        	numError.alert += alertData[error].length;
        };
        var stallData = this.requestNotPulledAlert();
        numError.stalled = 0;
        for (var error in stallData) {
        	numError.stalled += stallData[error].length;
        };
        return numError;
    };
    return tier1Requests;
};


WMStats.Requests.getPropertyByTask = function(taskPath, property, requestInfo) {
	
    if (requestInfo.TaskChain) {
        var numTasks = requestInfo.TaskChain;
        var taskSplit = taskPath.split("/");
        var task = taskSplit[taskSplit.length -1];
        for (var i=1; i <= numTasks; i++) {
            if (requestInfo["Task" + i].TaskName == task) {
                if (requestInfo["Task" + i][property] !== undefined) {
                	return requestInfo["Task" + i][property];
                } else {
                	break;
                }
            }
        }
    }
    
    return requestInfo[property];
};

WMStats.Requests.getDictPropertyByTask = function(property, requestInfo) {
	
    if (requestInfo.TaskChain) {
        var numTasks = requestInfo.TaskChain;
        var pValue = {};
        for (var i=1; i <= numTasks; i++) {
        	var taskName = requestInfo["Task" + i].TaskName;
            if (requestInfo["Task" + i][property] !== undefined) {
            	pValue[taskName] = requestInfo["Task" + i][property];
            } else {
            	if (requestInfo[property]) {
	            	var value = requestInfo[property][taskName];
	            	if (value === undefined) {
	            		pValue[taskName] = requestInfo[property];
	            	} else {
	            		pValue[taskName] = value;
	            	}
	            }else{
	            	return requestInfo[property];
	            }
            }
        }
        return pValue;
    }
    
    return requestInfo[property];
};


