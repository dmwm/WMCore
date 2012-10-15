WMStats.namespace('JobDetailList');
(function() { 
    var format = function (data) {
        var htmlstr = "";
        var jobDetails = data.getData();
        for (var index in jobDetails) {
            var jobDoc = jobDetails[index];
            htmlstr += "<div class='jobDetailBox' id='jobDetail-" + index + "'>"
            htmlstr += "<ul>";
            htmlstr += "<li> job name: " + jobDoc._id + "</li>";
            htmlstr += "<li> wmbs job id: " + jobDoc.wmbsid + "</li>";
            htmlstr += "<li> workflow: " + jobDoc.workflow + "</li>";
            htmlstr += "<li> task: " + jobDoc.task + "</li>";
            htmlstr += "<li> state: " + jobDoc.state + "</li>";
            htmlstr += "<li> site: " + jobDoc.site + "</li>";
            htmlstr += "<li> exit code: " + jobDoc.exitcode + "</li>";
            htmlstr += "<li> retry count: " + jobDoc.retrycount + "</li>";
            htmlstr += "<li> lumis:" 
            for (var i in jobDoc.lumis) {
                for (var j in jobDoc.lumis[i]) {
                    htmlstr += jobDoc.lumis[i][j] + " "
                }
                htmlstr +=  "\n "
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
                htmlstr += "<ul>";
                htmlstr += "<li> lfn:" + jobDoc.output[i].lfn +"</li>";
                
                htmlstr += "<li> location: ";
                htmlstr += jobDoc.output[i].location;
                /*
                for (var j in jobDoc.output[i].location) {
                    htmlstr += jobDoc.output[i].location[j] + " ";
                }*/
                htmlstr += "</li>";
                htmlstr += "<li> checksums: adler32: " + jobDoc.output[i].checksums.adler32 + 
                            " cksum: " + jobDoc.output[i].checksums.cksum + "</li>";
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
    
    
    WMStats.JobDetailList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    }
})()
