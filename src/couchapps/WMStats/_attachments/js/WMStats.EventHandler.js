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
        
        // 1. highlight row
        $('#campaignTable tbody tr').removeClass('green')
        $(this).addClass('green')
        
        // 2. create the request table
        var nTds = $('td', this);
        var campaignName = $(nTds[0]).text();
        var options = {'startkey':[campaignName], 'endkey':[campaignName, {}], 
                       'include_docs': true};
        WMStats.RequestView.createTable("#requestDiv", 
                                        'requestByCampaignAndDate', options)
                                        
        // 3. clean up job summary and detail view. 
        $("#jobDiv").empty();
        $("#jobDetailDiv").empty();
    } );
    
    //request table event handler
    // when table row is clicked create job summary
    $('#requestTable tbody tr').live('click', function () {
        // 1. highlight row
        $('#requestTable tbody tr').removeClass('green')
        $(this).addClass('green')
        
        // 2. create the job summary table
        var nTds = $('td', this);
        var requestName = $(nTds[0]).text();
        WMStats.JobSummaryView.createSummaryView("#jobDiv", requestName) 
        
        // 3. clean up job detail view.
        $("#jobDetailDiv").empty();
    } );
    
    // job summary table event handler
    // when table row is clicked create job summary
    $('#jobSummaryTable tbody tr').live('click', function () {
        
        // 1. highlight row
        $('#jobSummaryTable tbody tr').removeClass('green')
        $(this).addClass('green')
        
        // 2. create the job detail view
        var nTds = $('td', this);
        var summary = {};
        summary.workflow = $('#jobSummaryTable').data("workflow");
        summary.status = $(nTds[0]).text();
        summary.exitCode = Number($(nTds[3]).text());
        WMStats.JobDetailView.createDetailView("#jobDetailDiv", summary);
    } );
    
    // job summary event handler
    // when jobsummary is clicked create job detail view
    $('#jobDiv:nth-child(1n)').live('click', function (event) {
        var summary =  $(event.currentTarget).data('summary');
        if (summary) {
            WMStats.JobDetailView.createDetailView("#jobDetailDiv", summary);
        }
    } );
    
    
    // collapsible bar
    $('div.caption img').live('click', function(event){
        $(this).parent('div.caption').siblings('div.body').toggle('nomal');
     });

}(jQuery)
