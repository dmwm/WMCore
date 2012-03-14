WMStats.namespace("EventHandler")

/*
 * Add EventHandler 
 */
WMStats.EventHandler = function($) {
    // campaign table event handler.
    // when table row is clicked create the request table.
    $('#campaignTable tbody tr').live('click', function () {
        var nTds = $('td', this);
        var campaignName = $(nTds[0]).text();
        var options = {'startkey':[campaignName], 'endkey':[campaignName, {}], 
                       'include_docs': true};
        WMStats.RequestView.createTable("#requestDiv", 
                                        'requestByCampaignAndDate', options) 
    } );
    
    //request table event handler
    // when table row is clicked create job summary
    $('#requestTable tbody tr').live('click', function () {
        var nTds = $('td', this);
        var requestName = $(nTds[0]).text();
        WMStats.JobSummaryView.createSummaryView("#jobDiv", requestName) 
    } );
}(jQuery)
