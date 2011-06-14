WMCore.namespace("GlobalMonitor.RequestMonitor");
/*
 * Overview.js displaces the information gathered from request manager,
 * global queue , local queue and couchDB.
 *
 * All the record are acquired from database on each service (i.e global queue
 * rest service not from the user input).
 */
WMCore.GlobalMonitor.RequestMonitor.overviewTable = function(divID, filterDiv,
                                                        filterFunction){

    var postfixLink = "/template/ElementSummaryByWorkflow?workflow=";

    var formatRequest = function(elCell, oRecord, oColumn, sData) {
            elCell.innerHTML = "<a href='../view/details/" + sData  +
                                        "' target='_blank'>" + sData + "</a>";
        };

    var formatGlobalQ = function(elCell, oRecord, oColumn, sData) {
            var host;
            if (!sData) {
                elCell.innerHTML = "Not Assigned";
            } else {
                host = sData.split('/')[2];
                elCell.innerHTML = "<a href='" + sData  + "monitor" + postfixLink +
                                     oRecord.getData("request_name") + "' target='_blank'>" +
                                     host + "</a>";
            };
        };

    var formatLocalQ = function(elCell, oRecord, oColumn, sData) {
            var host;
            if (!sData || ! sData.length) {
                elCell.innerHTML = "Not Assigned";
            } else {
            for (data in sData) {
                host = sData[data].split('/')[2];
                elCell.innerHTML = "<a href='" + sData + "monitor" + postfixLink +
                         oRecord.getData("request_name") + "' target='_blank'>" +
                         host + "</a> <br>";
                };
        };
        };

    var formatCouchDB = function(elCell, oRecord, oColumn, sData) {
            var host;
            if (oRecord.getData("error")) {
                 elCell.innerHTML = "<font color='red'> " + oRecord.getData("error") + "<font>";
                 return;
            };
            if (!sData) {
                if (oRecord.getData("couch_error")) {
                    elCell.innerHTML = "<font color='red'> Can't connect Job DB <font>";
                } else {
                    elCell.innerHTML = "No jobs in DB";
                }
            } else {
                host = "CouchDB Link";
                elCell.innerHTML = "<a href='" + sData + "' target='_blank'>" + host + "</a>";
            };
        };

    var formatJobLink = function(elCell, oRecord, oColumn, sData, type) {
            var couchLink
            if (!sData) {
                elCell.innerHTML = 0;
            } else {
                couchLink = oRecord.getData("couch_job_info_base");
                if (couchLink) {
                    elCell.innerHTML = "<a href='" + couchLink.replace("replace_to_", type) +
                                     "' target='_blank'>" + sData + "</a>";
                } else {
                    elCell.innerHTML = sData;
                };
            };
        };
    var formatPending = function(elCell, oRecord, oColumn, sData) {
            formatJobLink(elCell, oRecord, oColumn, sData, "pending")
    };

    var formatRunning = function(elCell, oRecord, oColumn, sData) {
            formatJobLink(elCell, oRecord, oColumn, sData, "running")
    };

    var formatCoolOff = function(elCell, oRecord, oColumn, sData) {
            formatJobLink(elCell, oRecord, oColumn, sData, "cooloff")
    };

    var formatSuccess = function(elCell, oRecord, oColumn, sData) {
            formatJobLink(elCell, oRecord, oColumn, sData, "success")
    };

    var formatFailure = function(elCell, oRecord, oColumn, sData) {
            formatJobLink(elCell, oRecord, oColumn, sData, "failed")
    };

    var formatBatch = function(elCell, oRecord, oColumn, sData) {
            formatJobLink(elCell, oRecord, oColumn, sData, "running")
    };

    var createProgressBar = function(elLiner, result, total, container) {
        //if total is 0 make 0% complete;
          if (total === 0 || total === null) {
              total = 1;
              rTotal = 0;
          } else {
              rTotal = total;
          };
          if (result === null) {
              result = 0;
          };
          percent = result/total*100;
          elLiner.innerHTML = "<div class='percentDiv'>" + percent.toFixed(1) +
                                "% (" + result + '/' + rTotal + ")</div>";
          var pb = new YAHOO.widget.ProgressBar({
                     width:'100px',
                     height:'8px',
              maxValue: total,
                     //className:'some_other_image',
                     value:result
           });
           pb.render(elLiner);
           container.push(pb);
    };

    var pbs = [];
    var progressFormatter = function (elLiner, oRecord, oColumn, oData) {
          var total = (oRecord.getData("pending") +
                       oRecord.getData("running") + oRecord.getData("cooloff") +
                       oRecord.getData("success") + oRecord.getData("failure"));

          var completed = oRecord.getData("success") + oRecord.getData("failure");

          createProgressBar(elLiner, completed, total, pbs);
    };

    var pbq = [];
    var queueProgressFormatter = function (elLiner, oRecord, oColumn, oData) {
          var total = oRecord.getData("total_jobs");
          var inWMBS = oRecord.getData("inWMBS");

          createProgressBar(elLiner, inWMBS, total, pbq);
    };


    var dataSchema = {
        fields: [{key: "request_name"},
                 {key: "status"},
                 {key: "type"},
                 {key: "global_queue"},
                 {key: "local_queue"},
                 {key: "pending"},
                 {key: "cooloff"},
                 {key: "running"},
                 {key: "success"},
                 {key: "failure"},
                 //batch job status. (capital letters
                 //needs better distinction
                 {key: "Pending"},
                 {key: "Running"},
                 {key: "Complete"},
                 {key: "Error"},
                 {key: "inQueue"},
                 {key: "inWMBS"},
                 {key: "total_jobs"},
                 {key: "couch_doc_base"},
                 {key: "couch_job_info_base"},
                 {key: "couch_error"},
                 {key: "error"}
                 ]
        };

   var dataTableCols = [{key: "request_name", label: "request name", formatter:formatRequest},
                 {key: "status"},
                 {key: "type"},
                 {key: "global_queue", formatter:formatGlobalQ},
                 {key: "local_queue", formatter:formatLocalQ},
                 {key: "pending", label: "queued", formatter:formatPending},
                 {key: "cooloff", label: "cool off", formatter:formatCoolOff},
                 //{key: "running", label: "submitted", formatter:formatRunning},

                 //batch status for running (submitted jobs) needs to match the number
                 {key: "Pending", label: "pending", formatter:formatBatch},
                 {key: "Running", label: "running", formatter:formatBatch},
                 //{key: "Running", label: "completed", formatter:formatBatch},
                 //{key: "Error",label: "error", formatter:formatBatch},

                 {key: "success", formatter:formatSuccess},
                 {key: "failure", formatter:formatFailure},
                 {key: "couch_doc_base", label: "summary", formatter:formatCouchDB},
                 {key: "job completion", formatter:progressFormatter},
                 {key: "queue injection", formatter:queueProgressFormatter}
                 ];

    var dataUrl = "/reqmgr/monitorSvc/requestmonitor";
    var dataSource = WMCore.createDataSource(dataUrl, dataSchema);

    var tableInfo = {};
    tableInfo.divID = divID;
    tableInfo.cols = WMCore.createDefaultTableDef(dataTableCols);
    tableInfo.conf = {};
    // Set up pagination
    tableInfo.conf.paginator = new YAHOO.widget.Paginator(
                                    {rowsPerPage : 25,
                                     containers : 'pagediv'});

    dataSource.doBeforeCallback = function (req,raw,res,cb) {
            // This is the filter function
            var rawData     = res.results || [];
            filterFunction(filterDiv, rawData, dataSchema, tableInfo);
            return res;
    };
    dataSource.sendRequest();
    dataSource.setInterval(600000, null, {});
};
