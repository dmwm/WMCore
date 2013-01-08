WMStats.namespace("WorkloadSummaryController");

(function($){
    
    $(document).on('click', '#WorkloadSummarySearchButton', function(event) {
        var selectedSearch = $('#search_option_board select[name="SearchOptions"] :selected').val();
        var searchStr = $('input[name="workloadSummarySearch"]').val();
        var view;
        var options =  {'include_docs': true, 'reduce': false};
        if (selectedSearch === 'request') {
            view = "allDocs";
            options.key = searchStr;
        }else if (selectedSearch === 'outputdataset') {
            view = "summaryByOutputdataset";
            options.key = searchStr;
        }else if (selectedSearch === 'inputdataset') {
            view = "summaryByInputdataset";
            options.key = searchStr;
        }
        WMStats.WorkloadSummaryModel.retrieveData(view, options);
        event.stopPropagation();
    })
    
    var E = WMStats.CustomEvents;
    $(WMStats.Globals.Event).on(E.WORKLOAD_SUMMARY_READY, 
        function(event, data) {
            var data = WMStats.WorkloadSummaryModel.getData();
            // draw alert
            WMStats.WorkloadSummaryTable(data, "#search_result_board");
        })

})(jQuery);
