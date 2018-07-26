WMStats.namespace("WorkloadSummaryTable");

WMStats.WorkloadSummaryTable = function (data, containerDiv) {

    var formatReqDetailUrl = WMStats.Utils.formatReqDetailUrl;
    var formatWorkloadSummarylUrl = WMStats.Utils.formatWorkloadSummarylUrl;

    var tableConfig = {
        "pageLength": 25,
        "scrollX": true,
        "dom": 'lfrtip',
        "columns": [
            /*
            {"title": "D", 
             "defaultContent": 0,
             "width": "15px",
             "render": function (data, type, row, meta) {
                            return WMStats.Utils.formatDetailButton("detail");
                        }},
            {"title": "L", 
             "defaultContent": 0,
             "width": "15px",
             "render": function (data, type, row, meta) {
                            return WMStats.Utils.formatDetailButton("drill");
                        }},
            */
            { "data": "workflow", "title": "workflow",
             "render": function (data, type, row, meta) {
                            return formatReqDetailUrl(row._id);
                      },
              "width": "150px"
            },
            {"render": function (data, type, row, meta) {
              				if (type === "display") {
                            	return formatWorkloadSummarylUrl(row._id, 
                                	row.request_status[row.request_status.length -1].status);
                            }
                            return row.request_status[row.request_status.length -1].status;
                          },
            },
            { "data": "request_type", "title": "type", "defaultContent": ""},
            { "data": "priority", "title": "priority", "defaultContent": 0},
            { "data": "campaign", "title": "campaign", "defaultContent": ""}
        ]
    };
    
    var filterConfig = {};
    
    tableConfig.data = data.getData();
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};

(function() {
    
     var vm = WMStats.ViewModel;
     
     vm.SearchPage.subscribe("data", function(){
        WMStats.WorkloadSummaryTable(vm.SearchPage.data(), "#search_result_board");
     });
})();
    