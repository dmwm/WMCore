WMStats.namespace("AlertConfig");
WMStats.namespace("AlertTable");

WMStats.AlertConfig = function(data) {
        // jquery datatable config
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "request"},
            { "mDataProp": "count", "sTitle": "cooloff jobs"}
        ]
    }
    
    tableConfig.aaData = data.getData();;
    
    var filterConfig = {};
    
    return {
        tableConfig: tableConfig,
        filterConfig: filterConfig
    }
}

WMStats.AlertTable = function (data, containerDiv) {
        var config = WMStats.AlertConfig(data);
        return WMStats.Table(config.tableConfig).create(containerDiv, 
                                                 config.filterConfig);
}