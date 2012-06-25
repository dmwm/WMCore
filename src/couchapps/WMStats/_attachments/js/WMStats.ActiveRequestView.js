WMStats.namespace("ActiveRequestView");
(function() {
    var initView = 'requestByStatus'; 
    var options = {'keys': [
                            "new",
                            //"testing-approved",
                            //"testing",
                            //"tested",
                            //"test-failed",
                            "assignment-approved",
                            "assigned",
                            "ops-hold",
                            "negotiating",
                            "acquired",
                            "running",
                            "failed",
                            "epic-FAILED",
                            "completed",
                            "closed-out",
                            //"announced",
                            //"aborted",
                            //"rejected"
                            ], 
                   'include_docs': true};

    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatReqDetailUrl(o.aData.workflow);
                      },
              "bUseRendered": false
            },
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "mDataProp": "priority", "sTitle": "priority", "sDefaultContent": 0},
            { "sDefaultContent": 0,
              "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              return (_get(o.aData, "status.inWMBS",  0) / 
                                      _get(o.aData, 'total_jobs', 1) * 100 + '%');
                        }
            },
            { "sDefaultContent": 0,
              "sTitle": "job progress", 
              "fnRender": function ( o, val ) {
                            return (((_get(o.aData, "status.success", 0) + _failureTotal(o.aData)) /
                                     _WMBSJobsTotal(o.aData) * 100)  + "%");
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure rate", 
              "fnRender": function ( o, val ) {
                            return ((_failureTotal(o.aData) /
                                    (_get(o.aData, "status.success", 0) + _failureTotal(o.aData)) * 100)  + "%");
                          }
            },
            /*
            { "sDefaultContent": 0,
              "sTitle": "EAT", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.running", 0);
                          }
            },
            */
            //TODO add more data (consult dataops)
        ]
    }
    WMStats.ActiveRequestView = new WMStats._RequestViewBase(initView, options, WMStats.DefaultRequestTable);
})()
