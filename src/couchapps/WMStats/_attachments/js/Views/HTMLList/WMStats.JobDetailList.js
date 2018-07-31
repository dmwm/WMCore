WMStats.namespace('JobDetailList');
(function() { 
    
    var stateTransitionFormat = function(state) {
        return "<b>" + state['newstate'] + ":</b> " + 
                WMStats.Utils.utcClock(new Date(state['timestamp'] * 1000)) + 
                ",  " + state['location'];
    };
    
    var inputFileFormat = function(inputFile) {
        return inputFile['lfn'];
    };
    
    var lumiFormat = function(lumis) {
        
        function lumiRangeFormat() {
            if (startLumi == preLumi) {
               lumiFormat.push("[" + startLumi + "]");
            } else {
               lumiFormat.push("[" + startLumi + " - " + preLumi + "]");
            };
        };

        var preLumi = null;
        var startLumi = null;
        var lumiFormat = new Array();
        for (var i in lumis) {
            for (var j in lumis[i]) {
                for (var k in lumis[i][j]) {
                    var currentLumi = Number(lumis[i][j][k]);
                    if (startLumi === null) {
                        startLumi = currentLumi;
                    } else if ((preLumi + 1) !== currentLumi) {
                            lumiRangeFormat();
                            startLumi = currentLumi;
                    };
                    preLumi = currentLumi;
                };
            };
       };
       
       if (startLumi !== null) {
           lumiRangeFormat();
       };
       return lumiFormat;
    };
    
    var logArchiveFormat = function(archiveObj, key) {
        return key;
    };
    
    var format = function (data) {
        var jobDetails = data.getData();
        var requestData = WMStats.ActiveRequestModel.getData();
        var htmlstr = '<div><nav id="jobDetailNav" class="button-group"><ul>';
        var jobDivIDPrefix = "jobDetail-";
        for (var index in jobDetails) {
            var buttonNumber = Number(index) + 1;
            if (buttonNumber == 1) {
                htmlstr += '<li><a href="#' + jobDivIDPrefix + index + '" class="pageButton button-selected">' + buttonNumber +' </a></li>';
            } else {
                htmlstr += '<li><a href="#' + jobDivIDPrefix + index + '" class="pageButton button-unselected">' + buttonNumber +' </a></li>';
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
            htmlstr += "<li><b>Input dataset:</b> " + WMStats.Utils.getInputDatasets(requestData.getData(jobDoc.workflow)) + "</li>";
            if (typeof jobDoc.site == "object") {
                htmlstr += "<li><b>Site:</b> N/A </li>";
            } else {
                htmlstr += "<li><b>Site:</b> " + jobDoc.site + "</li>";
            }
            htmlstr += "<li><b>Agent:</b> " + jobDoc.agent_name + "</li>";
            htmlstr += "<li><b>ACDC URL:</b> " + jobDoc.acdc_url + "</li>";
            htmlstr += "<li>" + WMStats.Utils.expandFormat(jobDoc.state_history, "State Transition", stateTransitionFormat) + "</li>";
            htmlstr += "<li><b>Exit code:</b> " + jobDoc.exitcode + "</li>";
            htmlstr += "<li><b>Retry count:</b> " + jobDoc.retrycount + "</li>";
            htmlstr += "<li><b>Errors:</b> "; 
            for (var errorType in jobDoc.errors) {
                htmlstr += "<ul>";
                htmlstr += "<li><b>" + errorType + "</b></li>";
                for (var i in jobDoc.errors[errorType]){
                    htmlstr += "<ul>";
                    htmlstr += "<li><b>" + jobDoc.errors[errorType][i].type +" (Exit Code: " + jobDoc.errors[errorType][i].exitCode + ")</b></li>";
                    htmlstr += "<ul>";
                    htmlstr += "<li><pre>" + WMStats.Utils.escapeHtml(jobDoc.errors[errorType][i].details) +"</pre></li>";
                    htmlstr += "</ul>";
                    htmlstr += "</ul>";
                }
                 htmlstr += "</ul>";
            } 
            htmlstr += "</li>";
            
            htmlstr += "<li>" + WMStats.Utils.expandFormat(jobDoc.inputfiles, "Input files", inputFileFormat) + "</li>";
            htmlstr += "<li>" + WMStats.Utils.expandFormat(lumiFormat(jobDoc.lumis), "Lumis") + "</li>";
            
            htmlstr += "<li><b>Output:</b> "; 
            for (var i in jobDoc.output) {
                htmlstr += "<ul>";
				htmlstr += "<li><b>" + jobDoc.output[i].type + "</b></li>";
                htmlstr += "<ul>";
                htmlstr += "<li><b>lfn:</b> " + jobDoc.output[i].lfn +"</li>";
                
                htmlstr += "<li><b>location:</b> ";
                htmlstr += jobDoc.output[i].location;
                if (jobDoc.output[i].type === "logArchive") {
                    htmlstr += "<li class='pfn'><b>pfn:</b> ";
                    // call phedex to get lfn
                }
                /*
                for (var j in jobDoc.output[i].location) {
                    htmlstr += jobDoc.output[i].location[j] + " ";
                }*/
                htmlstr += "</li>";
                htmlstr += "<li><b>checksums: adler32:</b> " + jobDoc.output[i].checksums.adler32 + "</li>";
                htmlstr += "<li><b>size:</b> " + jobDoc.output[i].size + "</li>";
                htmlstr += "</ul>";
                htmlstr += "</ul>";
            }
            htmlstr += "</li>";
            if (jobDoc.worker_node_info) {
                htmlstr += "<li><b>Worker Node:</b>" + jobDoc.worker_node_info.HostName  + "</li>";
            };

            htmlstr += "<li>" + WMStats.Utils.expandFormat(jobDoc.logArchiveLFN, "log archive", logArchiveFormat) + "</li>";
            if (jobDoc.eos_log_url) {
                htmlstr += '<li><b>EOS log URL:</b><a href="' + jobDoc.eos_log_url.replace("eoscmshttp", "eoscmsweb") + '"> download log </a></li>';
            }
            htmlstr += "</ul>";
            htmlstr += "</div>";
        }
        return htmlstr;
    };
    
    
    WMStats.JobDetailList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    };
    
    // control job Detail
    
    var vm = WMStats.ViewModel;
    
    vm.JobDetail.subscribe("data", function() {
        WMStats.JobDetailList(vm.JobDetail.data(), vm.JobDetail.id());
    });
    /*
    vm.AlertJobDetail.subscribe("data", function() {
        WMStats.JobDetailList(vm.AlertJobDetail.data(), vm.AlertJobDetail.id());
    });
    */
    $(document).on('click', "#jobDetailNav li a", function(event){
        $('div.jobDetailBox').hide();
        $(this.hash).show();
        //TODO call phedex to get PFN
        
        $("#jobDetailNav li a").removeClass("button-selected").addClass("button-unselected");
        $(this).removeClass("button-unselected").addClass("button-selected");
        event.preventDefault();
    });
    
    $(WMStats.Globals.Event).on(WMStats.CustomEvents.PHEDEX_PFN_SUCCESS, 
        function(event, requestName) {
            $('#acdc_submission div.requestDetailBox').append(WMStats.Utils.formatReqDetailUrl(requestName));
    });
})();
