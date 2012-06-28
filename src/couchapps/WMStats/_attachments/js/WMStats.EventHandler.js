(function($){
    // Rewqest view search event handler
    $('input').live('keyup', function() {
                        var a=$('input').serializeArray(), filter={};
                        $.each(a, function(i, obj){
                            filter[obj.name] = obj.value;
                            });
                    var requestData = WMStats.ActiveRequestView.getData();
                    requestData.setFilter(filter);
                    $("#tab-active-request > div[name='requestSummary']").empty();
                    WMStats.ActiveRequestTable(requestData.filterRequests(), "#tab-active-request > div[name='requestSummary']");
                    })
})(jQuery);
