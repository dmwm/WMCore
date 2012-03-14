WMStats.namespace("JobSummaryView")

WMStats.JobSummaryView = (function() {
    /*
     * Create JobSummary view
     */
    //div container for job summary
    var _containerDiv = null;
    // default couch view name for job summary
    var _viewName = 'jobsByStatusWorkflow';
    
    function _formatHtml(jobSummary) {
        var htmlstr = "";
        htmlstr += jobSummary.workflow + ":\n"
        htmlstr += "<ol>"
        for (var index in jobSummary.status) {
            htmlstr += "<li><ul>";
            htmlstr += "<li> status: " + jobSummary.status[index].status + "</li>";
            htmlstr += "<li> site: " + jobSummary.status[index].site + "</li>";
            htmlstr += "<li> exitCode: " + jobSummary.status[index].exitCode + "</li>";
            htmlstr += "<li> error message: " 
            for (var i in jobSummary.status[index].errorMsg) {
                htmlstr += jobSummary.status[index].errorMsg[i] + " "
            } 
            htmlstr += "</li>";
            htmlstr += "<li> num of jobs: " + jobSummary.status[index].count + "</li>";
            htmlstr += "</ul></li>";
        }
        htmlstr += "</ol>"
        
        return htmlstr;
    }
                
    function _formatJobSummary(data) {
        var htmlstr = "";
        var jobSummary = {};
        for (var i in data.rows){
            jobSummary.workflow = data.rows[i].key[0];
            jobSummary.status = [];
            
            var statusSummary = {};
            statusSummary.status = data.rows[i].key[1];
            statusSummary.exitCode = data.rows[i].key[2];
            statusSummary.errorMsg = data.rows[i].key[3];
            statusSummary.site = data.rows[i].key[4];
            statusSummary.count = data.rows[i].value;
            jobSummary.status.push(statusSummary)
                     
        }
        $(_containerDiv).html(_formatHtml(jobSummary));
        
    }
    
    function createSummaryView(selector, workflow) {
        _containerDiv = selector;
        options = {'reduce': true, 'group_level': 5, 'startkey':[workflow], 
                   'endkey':[workflow, {}]};
        WMStats.Couch.view(_viewName, options, _formatJobSummary)
    }
    
    return {'createSummaryView': createSummaryView};
     
})();