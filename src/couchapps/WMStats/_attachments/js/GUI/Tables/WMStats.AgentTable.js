WMStats.namespace("AgentConfig");
WMStats.namespace("AgentTable");

WMStats.AgentConfig = function(data) {
        // jquery datatable config
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
            { "mDataProp": "timestamp", "sTitle": "last updated"}
        ]
    };
    tableConfig.aaData = data.getData();;
    
    var filterConfig = {};
    
    return {
        tableConfig: tableConfig,
        filterConfig: filterConfig
    }
}

WMStats.AgentTable = function (data, containerDiv) {
        var config = WMStats.AgentConfig(data);
        return WMStats.Table(config.tableConfig).create(containerDiv, 
                                                 config.filterConfig);
}