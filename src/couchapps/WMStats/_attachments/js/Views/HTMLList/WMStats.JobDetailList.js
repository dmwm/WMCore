WMStats.namespace('JobDetailList');
(function() { 
    var format = function (data) {
        var jobDetails = data.getData();
        var requestData = WMStats.ActiveRequestModel.getData();
        var htmlstr = '<div><nav id="jobDetailNav" class="button-group"><ul>';
        var jobDivIDPrefix = "jobDetail-";
        for (var index in jobDetails) {
            var buttonNumber = Number(index) + 1;
            if (buttonNumber == 1) {
                htmlstr += '<li><a href="#' + jobDivIDPrefix + index + '" class="pageButton button-selected">' + buttonNumber +' </a></li>'
            } else {
                htmlstr += '<li><a href="#' + jobDivIDPrefix + index + '" class="pageButton button-unselected">' + buttonNumber +' </a></li>'
            }
        }
        htmlstr += '</nav></ul></div>';
        for (var index in jobDetails) {
            var jobDoc = jobDetails[index];
            if (index === "0") {
                htmlstr += "<div class='jobDetailBox' id='" + jobDivIDPrefix + index + "'>";
            } else {
                htmlstr += "<div class='jobDetailBox hideDiv' id='" + jobDivIDPrefix + index + "'>";
            }
            htmlstr += "<ul>";
            htmlstr += "<li><b>Job Name:</b> " + jobDoc._id + "</li>";
            htmlstr += "<li><b>WMBS job id:</b> " + jobDoc.wmbsid + "</li>";
            htmlstr += "<li><b>Workflow:</b> " + jobDoc.workflow + "</li>";
            htmlstr += "<li><b>Task:</b> " + jobDoc.task + "</li>";
            htmlstr += "<li><b>Status:</b> " + jobDoc.state + "</li>";
            htmlstr += "<li><b>Input dataset:</b> " + requestData.getDataByWorkflow(jobDoc.workflow, "inputdataset", "") + "</li>";
            if (typeof jobDoc.site == "object") {
                htmlstr += "<li><b>Site:</b> N/A </li>";
            } else {
                htmlstr += "<li><b>Site:</b> " + jobDoc.site + "</li>";
            }
            htmlstr += "<li><b>State Transition:</b>"
            
            for (var i in jobDoc.state_history) {
                htmlstr += jobDoc.state_history[i]['newstate'] + ": " + jobDoc.state_history[i]['timestamp']
                htmlstr +=  ", "
            } 
            htmlstr += "</li>";
            
            htmlstr += "<li><b>Exit code:</b> " + jobDoc.exitcode + "</li>";
            htmlstr += "<li><b>Retry count:</b> " + jobDoc.retrycount + "</li>";
            htmlstr += "<li><b>Errors:</b> " 
            for (var errorType in jobDoc.errors) {
                htmlstr += "<ul>";
                htmlstr += "<li><b>" + errorType + "</b></li>";
                for (var i in jobDoc.errors[errorType]){
                    htmlstr += "<ul>";
                    htmlstr += "<li><b>" + jobDoc.errors[errorType][i].type +" (Exit Code: " + jobDoc.errors[errorType][i].exitCode + ")</b></li>";
                    htmlstr += "<ul>";
                    htmlstr += "<li><pre>" + jobDoc.errors[errorType][i].details +"</pre></li>";
                    htmlstr += "</ul>";
                    htmlstr += "</ul>";
                }
                 htmlstr += "</ul>";
            } 
            htmlstr += "</li>";
            
            htmlstr += "<li><b>Input Files:</b>"
            
            for (var i in jobDoc.inputfiles) {
                htmlstr += jobDoc.inputfiles[i].lfn + " ";
                htmlstr +=  "\n "
            } 
            htmlstr += "</li>";
            
            htmlstr += "<li><b>Lumis:</b>"
            
            for (var i in jobDoc.lumis) {
                for (var j in jobDoc.lumis[i]) {
                    htmlstr += jobDoc.lumis[i][j] + " "
                }
                htmlstr +=  "\n "
            } 
            htmlstr += "</li>";
            
            htmlstr += "<li><b>Output:</b> " 
            for (var i in jobDoc.output) {
                htmlstr += "<ul>";
                htmlstr += "<li><b>" + jobDoc.output[i].type + "</b></li>";
                htmlstr += "<ul>";
                htmlstr += "<li><b>lfn:</b> " + jobDoc.output[i].lfn +"</li>";
                
                htmlstr += "<li><b>location:</b> ";
                htmlstr += jobDoc.output[i].location;
                /*
                for (var j in jobDoc.output[i].location) {
                    htmlstr += jobDoc.output[i].location[j] + " ";
                }*/
                htmlstr += "</li>";
                htmlstr += "<li><b>checksums: adler32:</b> " + jobDoc.output[i].checksums.adler32 + 
                            ",<b>  cksum:</b> " + jobDoc.output[i].checksums.cksum + "</li>";
                htmlstr += "<li><b>size:</b> " + jobDoc.output[i].size + "</li>";
                htmlstr += "</ul>";
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
})();
