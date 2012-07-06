WMStats.namespace("Format")

WMStats.Format = function() {
    /*
     * sample job detail format
    {"_id":"jobid_test_workflow_1_0",
     "_rev":"1-b55d810cd7facf3352e0f3ce535bd4a3",
     "workflow":"test_workflow_1",
     "site":"T1_US_FNAL",
     "lumis":[],
     "errors":{"step1":{"out":{"type":"test error"}}},
     "retrycount":1,
     "task":"/test_workflow_1/task_0",
     "state":"complete",
     "output":[{"lfn":"/somewhere/file.root","location":"T1","checksums":"abc123","type":"test-type","size":"1000"}],
     "type":"jobsummary",
     "exitcode":666}}
    */
   
    function jobDetail(jobDoc, index) {
        htmlstr = "<div class='box' id='jobDetail-" + index + "'>"
        htmlstr += "<ul>";
        htmlstr += "<li> workflow: " + jobDoc.workflow + "</li>";
        htmlstr += "<li> task: " + jobDoc.task + "</li>";
        htmlstr += "<li> state: " + jobDoc.state + "</li>";
        htmlstr += "<li> site: " + jobDoc.site + "</li>";
        htmlstr += "<li> exit code: " + jobDoc.exitCode + "</li>";
        htmlstr += "<li> retry count: " + jobDoc.retryCount + "</li>";
        htmlstr += "<li> lumis: " 
        for (var i in jobDoc.lumis) {
            htmlstr += jobSummary.lumis[i] + " "
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
        return htmlstr;
    };
    
}
