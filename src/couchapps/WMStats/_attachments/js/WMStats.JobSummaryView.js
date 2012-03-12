WMStats.namespace("JobSummaryView")

WMStats.JobSummaryView = (function() {
    
    var _data = null;
    var _containerDiv = null;
    var _viewName = 'jobsByStatusWorkflow';
    var htmlstr = "";
    
    function _formatJobSummary(data) {
        for (var i in data.rows){
            var workflow = data.rows[i].key[0]
            var status = data.rows[i].key[1]
            var exitCode = data.rows[i].key[2]
            var errorMsg = data.rows[i].key[3]
            var site = data.rows[i].key[4]
            var count = data.rows[i].value;
            htmlstr += workflow + "\n" +
                       status + " "  + exitCode + " " +
                       errorMsg + " " + " " + site + " " +
                       count + "\n";         
        }
        $(_containerDiv).html(htmlstr);
    }
    
    function createSummaryView(selector, workflow) {
        _containerDiv = selector;
        options = {'reduce': true, 'group_level': 5, 'startkey':[workflow], 
                   'endkey':[workflow, {}]};
        WMStats.Couch.view(_viewName, options, _formatJobSummary)
    }
    
    return {'createSummaryView': createSummaryView};
     
})();