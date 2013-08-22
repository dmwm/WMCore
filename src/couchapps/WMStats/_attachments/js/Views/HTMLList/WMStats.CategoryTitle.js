WMStats.namespace('WMStats.CategoryTitle');
(function() { 
    
    var format = function (data) {
        var categoryKey = data;
        
        var htmlstr = "";
        //htmlstr += "<legend>request</legend>";
        htmlstr += "<div class='requestSummaryBox'>";
        htmlstr += "<ul>";
        htmlstr += "<li><b> Category: " + categoryKey + "</b></li>";
        htmlstr += "</ul>";
        htmlstr += "</div>";
        return htmlstr;
    };
    
    WMStats.CategoryTitle = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    };
})();
