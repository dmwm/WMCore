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
        } else if (selectedSearch === 'outputdataset') {
            view = "requestByOutputDataset";
            options.key = searchStr;
        } else if (selectedSearch === 'inputdataset') {
            view = "requestByInputDataset";
            options.key = searchStr;
        } else if (selectedSearch === 'prep_id') {
            view = "requestByPrepID";
            options.key = searchStr;
        } else if (selectedSearch === 'request_date') {
            view = "requestByDate";
            var beginDate = $('input[name="dateRange1"]').val().split("/");
            var endDate = $('input[name="dateRange2"]').val().split("/");
            options.startkey = [Number(beginDate[0]), Number(beginDate[1]), Number(beginDate[2])];
            options.endkey = [Number(endDate[0]), Number(endDate[1]), Number(endDate[2]), {}];
        }
        WMStats.RequestSearchModel.retrieveData(view, options);
        event.stopPropagation();
    })
    
    var E = WMStats.CustomEvents;
    $(WMStats.Globals.Event).on(E.WORKLOAD_SUMMARY_READY, 
        function(event, data) {
            var data = WMStats.RequestSearchModel.getData();
            // draw alert
            WMStats.WorkloadSummaryTable(data, "#search_result_board");
        })

})(jQuery);
