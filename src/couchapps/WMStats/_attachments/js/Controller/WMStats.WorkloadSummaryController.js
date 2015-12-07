WMStats.namespace("WorkloadSummaryController");

(function($){
    var vm = WMStats.ViewModel;
    var E = WMStats.CustomEvents;
    $(WMStats.Globals.Event).on(E.WORKLOAD_SUMMARY_READY, 
        function(event, data) {
            var data = WMStats.RequestSearchModel.getData();
            vm.SearchPage.data(data);
        });

})(jQuery);
