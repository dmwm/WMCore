WMStats.namespace("RequestTableDefaultConfig");
WMStats.namespace("DefaultRequestTable");

WMStats.RequestTableDefaultConfig = function(requestData) {
    
    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
    
    var defaultTableConfig = {
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
            { "mDataProp": "requestor", "sTitle": "requestor"},
            { "mDataProp": "request_type", "sTitle": "type"},
            { "mDataProp": "inputdataset", "sTitle": "inputdataset",
                           "sDefaultContent": ""},
            { "mDataProp": "site_white_list", "sTitle": "site white list",
                           "sDefaultContent": "",
              "fnRender": function ( o, val ) {
                            if (o.aData.site_white_list.length > 1) {
                                return "mulitple sites";
                            } else {
                                return o.aData.site_white_list[0];
                            }
                          },
              "bUseRendered": false,
            },
            { "mDataProp": "priority", "sTitle": "priority", "sDefaultContent": 0},
            { "sDefaultContent": 0,
              "sTitle": "queued", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getTotalQueued();
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending", 
              "fnRender": function ( o, val ) {
                            return requestData.getDataByWorkflow(o.aData.workflow, "status.submitted.pending", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "running", 
              "fnRender": function ( o, val ) {
                            return requestData.getDataByWorkflow(o.aData.workflow, "status.submitted.running", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure",
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getTotalFailure();
                          }
            },
            
            { "sDefaultContent": 0,
              "sTitle": "canceled", 
              "fnRender": function ( o, val ) {
                            return requestData.getDataByWorkflow(o.aData.workflow, "status.canceled", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "success",
              "fnRender": function ( o, val ) {
                            return requestData.getDataByWorkflow(o.aData.workflow, "status.success", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "cooloff", 
              "fnRender": function ( o, val ) {
                            var reqSummary = requestData.getSummary(o.aData.workflow);
                            return reqSummary.getTotalCooloff();
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "prev cooloff",
              "fnRender": function ( o, val ) {
                            return (requestData.getDataByWorkflow(o.aData.workflow, "status.submitted.retry", 0) + 
                                    requestData.getDataByWorkflow(o.aData.workflow, "status.queued.retry", 0));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              var result = (requestData.getDataByWorkflow(o.aData.workflow, "status.inWMBS",  0) / 
                                            requestData.getDataByWorkflow(o.aData.workflow, 'total_jobs', 1) * 100)
                              return (result.toFixed(1) + '%');
                        }
            }
            
            //TODO add more data (consult dataops)
        ]
    }
    
    var defaultFilterConfig = {
        "sPlaceHolder": "head:before",
        "aoColumns": [
            {type: "text", bRegex: true, bSmart: true},               
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true}
        ]
    }
    
    defaultTableConfig.aaData = requestData.getList();
    
    return {
        tableConfig : defaultTableConfig,
        filterConfig: defaultFilterConfig,
    }
};


WMStats.DefaultRequestTable = function (requestData, containerDiv) {
        var config = WMStats.RequestTableDefaultConfig(requestData);
        config.tableConfig.aaData = requestData.getList();
        var selector = containerDiv;
        //var tableSetting = '<table width=1600 cellpadding="0" cellspacing="0" border="0" class="display"></table>'
        return WMStats.Table(config.tableConfig).create(selector, 
                                                 config.filterConfig);
}
