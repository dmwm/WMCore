WMStats.namespace("EventHandler")

/*
 * Add EventHandler 
 */
WMStats.EventHandler = function($) {
    
    function highlightRow(selector, currenElement) {
        $(selector).removeClass('green');
        $(currenElement).addClass('green');
    }
 
    function tableRowClick(selector, func) {
        $(selector).live('click', function () {
            highlightRow(selector, this);
            func(this);
        });
    }
    
    function poplurateRequestTable(currentElement){
        // create the request table
        var nTds = $('td', currentElement);
        var campaignName = $(nTds[0]).text();
        var options = {'startkey':[campaignName], 'endkey':[campaignName, {}], 
                       'include_docs': true};
        WMStats.RequestView.createTable("#requestDiv", 
                                        'requestByCampaignAndDate', options)
                                        
        // clean up job summary and detail view. 
        $("#jobDiv").empty();
        $("#jobDetailDiv").empty();
    }
    
    function poplurateJobTable(currentElement){
        var nTds = $('td', currentElement);
        var requestName = $(nTds[0]).text();
        WMStats.JobSummaryView.createSummaryView("#jobDiv", requestName) 
        
        // 3. clean up job detail view.
        $("#jobDetailDiv").empty();
    }
    
    function poplurateJobDetail(currentElement){
        // 2. create the job detail view
        var nTds = $('td', currentElement);
        var summary = {};
        summary.workflow = $('#jobSummaryTable').data("workflow");
        summary.status = $(nTds[0]).text();
        summary.site = $(nTds[1]).text();
        summary.exitCode = Number($(nTds[2]).text());
        WMStats.JobDetailView.createDetailView("#jobDetailDiv", summary);
    }
    
    
    // actual event binding codes
    // row mouse over/ mouse out events
    $('tr').live('mouseover', function(event) {
        $(this).addClass('yellow')
    });
    
    $('tr').live('mouseout', function(event) {
        $(this).removeClass('yellow')
    });
    
    // collapsible bar
    $('div.caption img').live('click', function(event){
        $(this).parent('div.caption').siblings('div.body').toggle('nomal');
    });
    
    tableRowClick('#campaignTable tbody tr', poplurateRequestTable);
    tableRowClick('#requestTable tbody tr',  poplurateJobTable);
    tableRowClick('#jobSummaryTable tbody tr',  poplurateJobDetail);
    
    
    $('div').ajaxSend(function(){
            alert("test");
            $(this).show();
        }).live("ajaxComplete", function(){
            $(this).hide();
    });

}(jQuery)
