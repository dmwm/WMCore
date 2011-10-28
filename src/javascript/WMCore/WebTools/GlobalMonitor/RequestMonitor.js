WMCore.namespace("GlobalMonitor.RequestMonitor");
/*
 * Overview.js displaces the information gathered from request manager,
 * global queue , local queue and couchDB.
 *
 * All the record are acquired from database on each service (i.e global queue
 * rest service not from the user input).
 */
WMCore.GlobalMonitor.RequestMonitor.overviewTable = function(divID, filterDiv,
                                                        filterFunction, perPageID){

    var postfixLink = "/_design/WorkQueue/_rewrite/elementsInfo?request=";

    var formatRequest = function(elCell, oRecord, oColumn, sData) {
            var reqmgrUrl = WMCore.GlobalMonitor.Env['reqmgr_url'];
            elCell.innerHTML = "<a href='" + reqmgrUrl + "../view/details/" + sData  +
                                        "' target='_blank'>" + sData + "</a>";
        };

    var formatGlobalQ = function(elCell, oRecord, oColumn, sData) {
            var host;
            if (!sData) {
                elCell.innerHTML = "N/A";
            } else {
                elCell.innerHTML = "<a id='gq' href='" + sData + postfixLink +
                                     oRecord.getData("request_name") + "' target='_blank' title='" + sData +"'> GQ </a>";
                new YAHOO.widget.Tooltip("globalQ", { context:"gq" });
            };
    };

    var formatLocalQ = function(elCell, oRecord, oColumn, sData) {
            var host;
            if (!sData || ! sData.length) {
                elCell.innerHTML = "N/A";
            } else {
            for (data in sData) {
                elCell.innerHTML = "<a id='"+ sData + "' href='" + sData + postfixLink +
                         oRecord.getData("request_name") + "' target='_blank'  title='" + sData +"'>LQ</a> <br>";
                new YAHOO.widget.Tooltip("localQ", { context:sData });
                };
        };
    };
    
    var siteFormatter = function(elCell, oRecord, oColumn, sData) {
            if (!sData) {
                elCell.innerHTML = "";
            } else {
                if (sData.toString().search(',') < 0) {
                   elCell.innerHTML = sData;
                } else {
                   elCell.innerHTML = "<a id='sites' title='"+ sData + "'> Multiple Sites </a>";
                   new YAHOO.widget.Tooltip("sites", { context:"sites" });
                }
            };
    }

    var formatCouchDB = function(elCell, oRecord, oColumn, sData) {
            var host;
            if (oRecord.getData("error")) {
                 elCell.innerHTML = "<font color='red'> " + oRecord.getData("error") + "<font>";
                 return;
            };
            if (oRecord.getData("status") == "completed") {
                var workloadSummaryUrl = WMCore.GlobalMonitor.Env['workload_summary_url'] +
                "/_design/WorkloadSummary/_show/histogramByWorkflow/";
                
                elCell.innerHTML = "<a id='couchDB' href='" + workloadSummaryUrl +
                           oRecord.getData("request_name") + "' target='_blank' title='" +
                           workloadSummaryUrl + "'> summary </a>";
                return;
            }

            if (!sData) {
                if (oRecord.getData("couch_error")) {
                    elCell.innerHTML = "<font color='red'> Can't connect Job DB <font>";
                } else {
                    elCell.innerHTML = "No Jobs";
                }
            } else {
                elCell.innerHTML = "<a id='couchDB' href='" + sData + "' target='_blank' title='" + sData + "'> summary </a>";
                new YAHOO.widget.Tooltip("couchDB", { context:"couchDB" });
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

    var numParser = function(s) {
        if (s) {
            return YAHOO.util.DataSource.parseNumber(s);
        } else {
            return 0;
        };
    };
    
    var STATUS_LIST = {
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
    };

    var sortStatus =  function(a, b, desc) { 
        var comp = YAHOO.util.Sort.compare; 
        var compStatus = comp(STATUS_LIST[a.getData("status")], STATUS_LIST[b.getData("status")], desc); 
        return compStatus;
    };
     
    var dataSchema = {
        fields: [{key: "request_name"},
                 {key: "status"},
                 {key: "type"},
                 {key: "global_queue"},
                 {key: "local_queue"},
                 {key: "pending", parser: numParser},
                 {key: "cooloff", parser: numParser},
                 {key: "running", parser: numParser},
                 {key: "success", parser: numParser},
                 {key: "failure", parser: numParser},
                 //batch job status. (capital letters
                 //needs better distinction
                 {key: "Pending", parser: numParser},
                 {key: "Running", parser: numParser},
                 {key: "Complete", parser: numParser},
                 {key: "Error", parser: numParser},
                 {key: "inQueue", parser: numParser},
                 {key: "inWMBS", parser: numParser},
                 {key: "total_jobs", parser: numParser},
                 {key: "site_whitelist"},
                 {key: "couch_doc_base"},
                 {key: "global_queue"},
                 {key: "local_queue"},
                 {key: "couch_job_info_base"},
                 {key: "couch_error"},
                 {key: "error"}
                 ]
        };

   var dataTableCols = [{key: "request_name", label: "request name", formatter:formatRequest},
                 {key: "status", sortOptions: {sortFunction: sortStatus}},
                 {key: "type"},
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
                 {key: "queue injection", formatter:queueProgressFormatter},
                 {key: "site_whitelist", label:"SW", formatter:siteFormatter},
                 {key: "global_queue", label:"GQ", formatter:formatGlobalQ},
                 {key: "local_queue", label: "LQ", formatter:formatLocalQ}
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
            filterFunction(filterDiv, rawData, dataSchema, tableInfo, perPageID);
            return res;
    };
    dataSource.sendRequest();
    dataSource.setInterval(600000, null, {});
};
