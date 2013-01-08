WMStats.namespace("AlertTable");

WMStats.AlertTable = function (data, containerDiv) {
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "request"},
            { "mDataProp": "count", "sTitle": "cooloff jobs"}
        ]
    }
    
    tableConfig.aaData = data.getData();;
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
