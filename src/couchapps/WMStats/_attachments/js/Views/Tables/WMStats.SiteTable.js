WMStats.namespace("SiteTable");

WMStats.SiteTable = function (data, containerDiv) {
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "site", "sTitle": "site"},
            { "mDataProp": "agent_url", "sTitle": "agent",
               "fnRender": function ( o, val ) {
                            return decodeURIComponent(o.aData.agent_url);
                      }
            },
            { "mDataProp": "queued.first", "sTitle": "queued first", 
                           "sDefaultContent": 0 },
            { "mDataProp": "queued.retry", "sTitle": "queued retry", 
                           "sDefaultContent": 0 },
            { "mDataProp": "submitted.first", "sTitle": "submitted first", 
                           "sDefaultContent": 0 },
            { "mDataProp": "submitted.retry", "sTitle": "submitted retry", 
                           "sDefaultContent": 0 },
            { "mDataProp": "submitted.pending", "sTitle": "submitted pending", 
                           "sDefaultContent": 0 },
            { "mDataProp": "submitted.running", "sTitle": "submitted running", 
                           "sDefaultContent": 0 },
            { "mDataProp": "failure.create", "sTitle": "create fail", 
                           "sDefaultContent": 0 },
            { "mDataProp": "failure.submit", "sTitle": "submit fail", 
                           "sDefaultContent": 0 },
            { "mDataProp": "failure.exception", "sTitle": "exception fail", 
                           "sDefaultContent": 0 },
            { "mDataProp": "canceled", "sTitle": "canceled", 
                           "sDefaultContent": 0 },
            { "mDataProp": "success", "sTitle": "success", 
                           "sDefaultContent": 0 },
            { "mDataProp": "cooloff", "sTitle": "cool off", 
                           "sDefaultContent": 0 },
            { "mDataProp": "timestamp", "sTitle": "updated",
              "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDate(o.aData.timestamp);
                      }}
            //TODO add more data (consult dataops)
        ]
    };
    
    tableConfig.aaData = data.getData();
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
