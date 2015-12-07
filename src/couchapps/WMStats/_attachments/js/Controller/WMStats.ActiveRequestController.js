WMStats.namespace("ActiveRequestController");

(function($){
    
    var E = WMStats.CustomEvents;
   
    var vm = WMStats.ViewModel;
    
    $(WMStats.Globals.Event).on(E.REQUESTS_LOADED, 
        function(event) {
            vm.propagateUpdate();
        });

    $(WMStats.Globals.Event).on(E.AGENTS_LOADED, 
        function(event, agentData) {
            vm.propagateUpdate();
        });

	$(WMStats.Globals.Event).on(E.ERROR_LOG_LOADED, 
        function(event, logData) {
            vm.propagateUpdate();
        });
        
    $(WMStats.Globals.Event).on(E.JOB_SUMMARY_READY, 
        function(event, data) {
            vm.JobView.updateDataAndChild(data);
            //vm.AlertJobView.updateDataAndChild(data);
        });

    $(WMStats.Globals.Event).on(E.JOB_DETAIL_READY, 
		function(event, data) {
            vm.JobDetail.data(data);
        });

    $(WMStats.Globals.Event).on(E.RESUBMISSION_SUMMARY_READY, 
        function(event, data) {
            var rData = vm.Resubmission.formatResubmissionData(data);
            vm.Resubmission.data(rData);
    });

    $(WMStats.Globals.Event).on(E.LOADING_DIV_START, 
        function(event, data) {
            // TODO: need to update when partial_request happens)
            var count = vm.ActiveRequestPage.refreshCount();
            vm.ActiveRequestPage.refreshCount(count + 1);
            if (vm.page() === vm.ActiveRequestPage && vm.ActiveRequestPage.refreshCount() === 1) {
                     $('#loading_page').show();
                }
        });

    $(WMStats.Globals.Event).on(E.LOADING_DIV_END, 
        function(event, data) {
            $('#loading_page').hide();
        });
        
    $(WMStats.Globals.Event).on(E.AJAX_LOADING_START, 
        function(event, data) {
            $('#loading_page').show();
        });
    
})(jQuery);
