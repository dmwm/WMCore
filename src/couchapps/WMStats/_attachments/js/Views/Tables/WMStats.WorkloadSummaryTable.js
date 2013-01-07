WMStats.namespace("WorkloadSummaryTable");

WMStats.WorkloadSummaryTable = function (data, containerDiv) {

    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;

    var tableConfig = {
        "iDisplayLength": 10,
        "sScrollX": "",
        "sDom": 'lrtip',
        "bAutoWidth": false,
        "aoColumns": [
            /*
            {"sTitle": "D", 
             "sDefaultContent": 0,
             "sWidth": "15px",
             "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDetailButton("detail");
                        }},
            {"sTitle": "L", 
             "sDefaultContent": 0,
             "sWidth": "15px",
             "fnRender": function ( o, val ) {
                            return WMStats.Utils.formatDetailButton("drill");
                        }},
            */
            { "mDataProp": "_id", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData._id);
                      },
              "bUseRendered": false, "sWidth": "150px"
            },
            { "mDataProp": "campaign", "sTitle": "campaign", "sDefaultContent": ""}
        ]
    }
    
    var filterConfig = {}
    
    tableConfig.aaData = data.getData();
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
