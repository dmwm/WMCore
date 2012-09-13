 function _formatHtml(jobSummary) {
        $(_containerDiv).empty();
        var htmlstr = "";
        htmlstr += jobSummary.workflow + ":\n"
        for (var index in jobSummary.status) {
            htmlstr += "<div class='box' id='jobsummary-" + index + "'>"
            htmlstr += "<ul>";
            htmlstr += "<li> status: " + jobSummary.status[index].status + "</li>";
            htmlstr += "<li> site: " + jobSummary.status[index].site + "</li>";
            htmlstr += "<li> exitCode: " + jobSummary.status[index].exitCode + "</li>";
            htmlstr += "<li> error message: " 
            for (var i in jobSummary.status[index].errorMsg) {
                htmlstr += jobSummary.status[index].errorMsg[i] + " "
            } 
            htmlstr += "</li>";
            htmlstr += "<li> num of jobs: " + jobSummary.status[index].count + "</li>";
            htmlstr += "</ul>";
            htmlstr += "</div>";
            
            var summaryData = jobSummary.status[index];
            summaryData.workflow = jobSummary.workflow
            $(_containerDiv).append(htmlstr).data('summary', summaryData)
        }
        return htmlstr;
    }