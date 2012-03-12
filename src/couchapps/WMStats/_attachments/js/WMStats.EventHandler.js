 /* Add events */
WMStats.namespace("EventHandler")

WMStats.EventHandler = function($) {

    $('#campaignTable tbody tr').live('click', function () {
        var nTds = $('td', this);
        var campaignName = $(nTds[0]).text();
        var options = {'startkey':[campaignName], 'endkey':[campaignName, {}], 'include_docs': true};
        WMStats.RequestView.createTable("#requestDiv", 'campaign-request', options) 
    } );
    
     $('#requestTable tbody tr').live('click', function () {
        var nTds = $('td', this);
        var requestName = $(nTds[0]).text();
        WMStats.JobSummaryView.createSummaryView("#jobDiv", requestName) 
    } );
}(jQuery)
