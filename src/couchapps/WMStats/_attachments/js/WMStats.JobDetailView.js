WMStats.namespace("JobDetailView")

WMStats.JobDetailView = (function() {
    /*
     * Create JobSummary view
     */
    //div container for job summary
    var _containerDiv = null;
    // default couch view name for job summary
    var _viewName = 'jobsByStatusWorkflow';
    
    function _formatHtml(jobDetails) {
        var htmlstr = "";
        for (var index in jobDetails) {
            var jobDoc = jobDetails[index];
            htmlstr += "<div class='box' id='jobDetail-" + index + "'>"
            htmlstr += "<ul>";
            htmlstr += "<li> workflow: " + jobDoc.workflow + "</li>";
            htmlstr += "<li> task: " + jobDoc.task + "</li>";
            htmlstr += "<li> state: " + jobDoc.state + "</li>";
            htmlstr += "<li> site: " + jobDoc.site + "</li>";
            htmlstr += "<li> exit code: " + jobDoc.exitcode + "</li>";
            htmlstr += "<li> retry count: " + jobDoc.retrycount + "</li>";
            htmlstr += "<li> lumis: " 
            for (var i in jobDoc.lumis) {
                htmlstr += jobDoc.lumis[i] + " "
            } 
            htmlstr += "</li>";
            /*
            htmlstr += "<li> error message: " 
            for (var i in jobSummary.status[index].errorMsg) {
                htmlstr += jobSummary.status[index].errorMsg[i] + " "
            } 
            htmlstr += "</li>";
            */
            htmlstr += "<li> output: " 
            for (var i in jobDoc.output) {
                htmlstr += jobDoc.output[i].lfn;
                htmlstr += "<ul>";
                htmlstr += "<li> location: " + jobDoc.output[i].location + "</li>";
                htmlstr += "<li> checksums: " + jobDoc.output[i].checksums + "</li>";
                htmlstr += "<li> type: " + jobDoc.output[i].type + "</li>";
                htmlstr += "<li> size: " + jobDoc.output[i].size + "</li>";
                htmlstr += "</ul>";
            }
            htmlstr += "</li>"; 
            htmlstr += "</ul>";
            htmlstr += "</div>";
        }
        return htmlstr;
    }
                
    function _formatJobDetails(data) {
        var htmlstr = "";
        var jobDetails = [];
        for (var i in data.rows){
            jobDetails.push(data.rows[i].doc);                    
        }
        $(_containerDiv).html(_formatHtml(jobDetails));
    }
    
    function createDetailView(selector, summary) {
        _containerDiv = selector;
        options= {'include_docs': true, 'reduce': false, 
                  'startkey': [summary.workflow, summary.status, summary.exitCode],
                  'endkey': [summary.workflow, summary.status, summary.exitCode, {}],
                  'limit': 3};
                  
        WMStats.Couch.view(_viewName, options, _formatJobDetails);
    }
    
    return {'createDetailView': createDetailView};
     
})();