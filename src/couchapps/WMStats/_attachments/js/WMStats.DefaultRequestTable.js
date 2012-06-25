WMStats.namespace("RequestTableDefaultConfig");
WMStats.namespace("DefaultRequestTable");

WMStats.RequestTableDefaultConfig = function(requestData) {
    
    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;
    var _get = WMStats.Utils.get;
    var _queuedTotal = requestData.queuedTotal;
    var _failureTotal = requestData.failureTotal;
    
    
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
                            return (_queuedTotal(o.aData.workflow));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.pending", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "running", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.running", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure",
              "fnRender": function ( o, val ) {
                            return (_failureTotal(o.aData.workflow));
                          }
            },
            
            { "sDefaultContent": 0,
              "sTitle": "canceled", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.canceled", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "success",
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.success", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "cooloff", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.cooloff", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "prev cooloff",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.submitted.retry", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              return ((_get(o.aData, "status.inWMBS",  0) / 
                                      _get(o.aData, 'total_jobs', 1) * 100) + '%');
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
        var selector = containerDiv + " table";
        return WMStats.Table(config.tableConfig).create(selector, 
                                                 config.filterConfig);
}
