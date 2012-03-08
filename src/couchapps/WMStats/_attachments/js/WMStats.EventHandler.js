 /* Add events */
WMStats.namespace("EventHandler")

WMStats.EventHandler = function($) {

    $('#campaignTable tbody tr').live('click', function () {
        var nTds = $('td', this);
        var campaignName = $(nTds[1]).text();
        if (campaignName == 0) { 
            campaignName = "";
        }
        var options = {'startkey':[campaignName], 'endkey':[campaignName, {}], 'include_docs': true};
        WMStats.RequestView.createTable("#requestDiv", 'campaign-request', options) 
    } );
}(jQuery)
