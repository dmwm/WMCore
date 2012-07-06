(function($){
    // Rewqest view search event handler
    $("div[name='search'] input").live('keyup', function() {
                        var a=$('input').serializeArray(), filter={};
                        $.each(a, function(i, obj){
                            filter[obj.name] = obj.value;
                            });
                    var requestData = WMStats.ActiveRequestView.getData();
                    requestData.setFilter(filter);
                    $("#tab-active-request > div[name='requestSummary']").empty();
                    WMStats.ActiveRequestTable(requestData.filterRequests(), "#tab-active-request div[name='requestSummary']");
                    WMStats.RequestDataList(requestData.getFilteredSummary(), "#tab-active-request div[name='requestData']");
                    })
    $("#tab-active-request > div[name='requestSummary']").live('requestDataReady', 
                    function(event, requestData) {
                        WMStats.RequestDataList(requestData.getSummary(), "#tab-active-request div[name='requestData']");
                    })
})(jQuery);
