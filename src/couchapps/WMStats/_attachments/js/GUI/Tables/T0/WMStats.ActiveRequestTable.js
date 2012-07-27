WMStats.namespace("ActiveRequestConfig");
WMStats.namespace("ActiveRequestTable");

WMStats.ActiveRequestConfig = function(requestData) {
    
    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
    var _getData = requestData.getDataByWorkflow;
    var  _WMBSJobsTotal =  requestData.getWMBSJobsTotal
    var _queuedTotal = requestData.queuedTotal;
    var _failureTotal = requestData.failureTotal;
    var _eta = requestData.estimateCompletionTime;

    var tableConfig = {
        "iDisplayLength": 25,
        "sScrollX": "",
        "sDom": 'lrtip',
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow"},
            { "sDefaultContent": 0,
              "sTitle": "job progress", 
              "fnRender": function ( o, val ) {
                            var totalJobs = _WMBSJobsTotal(o.aData.workflow) || 1;
                            var result = (_getData(o.aData.workflow, "status.success", 0) + _failureTotal(o.aData.workflow)) /
                                     totalJobs * 100
                            return  (result.toFixed(1) + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending", 
              "fnRender": function ( o, val ) {
                           var result = _getData(o.aData.workflow, "status.submitted.pending", 0);
                           return result
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "running", 
              "fnRender": function ( o, val ) {
                           var result = _getData(o.aData.workflow, "status.submitted.running", 0);
                           return result
                          }
            },
            /*
            { "sDefaultContent": 0,
              "sTitle": "event progressed", 
              "fnRender": function ( o, val ) {
                           var result = Number(_getData(o.aData.workflow, "output_progress.0.events", 0)) 
                           return result
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure rate", 
              "fnRender": function ( o, val ) {
                           var totalJobs = (_getData(o.aData.workflow, "status.success", 0) + _failureTotal(o.aData.workflow)) || 1
                           var result = _failureTotal(o.aData.workflow) / totalJobs * 100;
                           return (result.toFixed(1)  + "%");
                          }
            },
            */
            /** TODO: this needs to be calulated in different method since there is no request_status in
             * Tier0 case
            { "sDefaultContent": 0,
              "sTitle": "Eestimated Completion", 
              "fnRender": function ( o, val ) {
                            return (WMStats.Utils.foramtDuration(_eta(o.aData.workflow)));
                          }
            },
            **/
            { "sDefaultContent": 0,
              "sTitle": "cool off ", 
              "fnRender": function ( o, val ) {
                            return (_getData(o.aData.workflow, "status.cooloff", 0));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "run", 
              "fnRender": function ( o, val ) {
                            return _getData(o.aData.workflow, "run", 0);
                          }
            }
            //TODO add more data (consult dataops)
        ]
    }

    tableConfig.aaData = requestData.getList();
    
    return {
        tableConfig : tableConfig,
        filterConfig: null,
    }
};


WMStats.ActiveRequestTable = function (requestData, containerDiv) {
        var config = WMStats.ActiveRequestConfig(requestData);
        return WMStats.Table(config.tableConfig).create(containerDiv, 
                                                 config.filterConfig);
}
