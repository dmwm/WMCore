WMStats.namespace("GenericController");

(function($) {
    var vm = WMStats.ViewModel;
 // collapsible bar
    function closeRequestDetail() {
        $("#request_view div.detail_data").hide('puff', {}, 500);
        $("#acdc_submission").hide('puff', {}, 500);
        vm.RequestDetail.open = false;
    };
    
    $(document).on('click', 'div.caption img', function(event){
        $(this).parent('div.caption').siblings('div.body').toggle('nomal');
    });

    $(document).on('click', 'div.closingButton', function(event){
        //$(this).parent('div').hide('puff', {}, 500);
        //TODO: generalize to all the button not just for request detail
        closeRequestDetail();
        event.preventDefault();
    });
   
    $(document).keyup(function(event) {
        if (vm.RequestDetail.open && event.keyCode == 27) {
            closeRequestDetail();
            event.preventDefault();
        }
    });
    
    
    WMStats.GenericController.switchDiv = function (showSelector, hideSelectors) {
        for (var i in hideSelectors) {
                $(hideSelectors[i]).hide();
        }
        $(showSelector).show();
    };

})(jQuery);
