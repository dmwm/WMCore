WMStats.namespace("EventHandler")

/*
 * Add EventHandler 
 */
WMStats.EventHandler = function($) {
    
    $('tr').live('mouseover', function(event) {
        $(this).addClass('yellow')
    });
    
    $('tr').live('mouseout', function(event) {
        $(this).removeClass('yellow')
    });
    
    // campaign table event handler.
    // when table row is clicked create the request table.
    $('#campaignTable tbody tr').live('click', function () {
        $('#campaignTable tbody tr').removeClass('green')
        $(this).addClass('green')
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
        $('#requestTable tbody tr').removeClass('green')
        $(this).addClass('green')
        var nTds = $('td', this);
        var requestName = $(nTds[0]).text();
        WMStats.JobSummaryView.createSummaryView("#jobDiv", requestName) 
    } );
}(jQuery)
