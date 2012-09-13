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
        summary.summaryStruct.length = 1;
        summary.jobStatus = this._get(doc, 'status', {})
        
        return summary;
    };
    
    return requestSummary;
}

WMStats.Requests = function(noFilterFlag) {
    var tier1Requests = new WMStats.GenericRequests(noFilterFlag);
    //Move out of Generic
    var statusOrder = {
        "new": 1,
        "testing-approved": 2,
        "testing": 3,
        "tested": 4,
        "test-failed": 5,
        "assignment-approved": 6,
        "assigned": 7,
        "ops-hold": 8,
        "negotiating": 9,
        "acquired": 10,
        "running": 11,
        "failed": 12,
        "epic-FAILED": 13,
        "completed": 14,
        "closed-out": 15,
        "announced": 16,
        "aborted": 17,
        "rejected": 18
    }
 
    tier1Requests.estimateCompletionTime = function(request) {
        //TODO need to improve the algo
        // no infomation to calulate the estimate completion time
        var aData = this.getDataByWorkflow(request);
        var reqSummary = this.getSummary(request);
        var completedJobs = reqSummary.getJobStatus("success") + reqSummary.getTotalFailure();
        if (completedJobs == 0) return -1;
        // get running start time.
        var requestStatus = this._get(aData, "request_status");
        var lastStatus = requestStatus[requestStatus.length - 1];
        
        //request is done
        if (lastStatus.status !== 'running') return 0;
        
        var totalJobs = reqSummary.getWMBSTotalJobs() - reqSummary.getJobStatus("canceled");
        // jobCompletion percentage 
        var completionRatio = (completedJobs / totalJobs);
        var queueInjectionRatio = reqSummary.getJobStatus("inWMBS") / this._get(aData, 'total_jobs', 1);
        var duration = Math.round(Date.now() / 1000) - lastStatus.update_time;
        var timeLeft = Math.round((duration / (completionRatio * queueInjectionRatio)) - duration);
        
        return timeLeft;
    }

    return tier1Requests;
}