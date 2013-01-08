WMStats.namespace("AgentTable");

WMStats.AgentTable = function (data, containerDiv) {
    
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "agent_url", "sTitle": "agent url",
              "fnRender": function ( o, val ) {
                            return decodeURIComponent(o.aData.agent_url);
                      }
            },               
            { "mDataProp": "status", "sTitle": "status"},
            { "mDataProp": "agent_team", "sTitle": "teams"},
            { "mDataProp": "down_components", "sTitle": "components down", 
              "sDefaultContent": ""},
            { "mDataProp": "timestamp", "sTitle": "last updated",
              "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDate(o.aData.timestamp);
                      }}
        ]
    };
    tableConfig.aaData = data.getData();;
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
