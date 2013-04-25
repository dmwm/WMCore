WMStats.namespace("WorkloadSummaryTable");

WMStats.WorkloadSummaryTable = function (data, containerDiv) {

    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;

    var tableConfig = {
        "iDisplayLength": 25,
        "sScrollX": "",
        "sDom": 'lfrtip',
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
            { "mDataProp": "workflow", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatReqDetailUrl(o.aData._id);
                      },
              "bUseRendered": false, "sWidth": "150px"
            },
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData._id, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "mDataProp": "request_type", "sTitle": "type", "sDefaultContent": ""},
            { "mDataProp": "priority", "sTitle": "priority", "sDefaultContent": 0},
            { "mDataProp": "campaign", "sTitle": "campaign", "sDefaultContent": ""}
        ]
    }
    
    var filterConfig = {}
    
    tableConfig.aaData = data.getData();
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};

(function() {
    
     var vm = WMStats.ViewModel;
     
     vm.SearchPage.subscribe("data", function(){
        WMStats.WorkloadSummaryTable(vm.SearchPage.data(), "#search_result_board");
     });
})();
    