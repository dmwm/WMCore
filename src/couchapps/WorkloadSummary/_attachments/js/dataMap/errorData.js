errorSummary = function(errors) {
    var summary = {};
    /*
    summary.totalJobs = 0;
    summary.totalFiles = 0;
    summary.totalFailureTime = 0;
    */
    for (var task in errors) {
        summary[task] = {};
        /*
        summary[task].totalJobs = 0;
        summary[task].totalFiles = 0;
        summary[task].failureTime = errors[task].failureTime;
        */
        for (var step in errors[task]) {
            if (step == "failureTime") {
                //failureTime isn not step, ignore that
                continue;
            }
            summary[task][step] = new Array();
            var i = 0;
            for (var code in errors[task][step]) {
                codeSummary = {};
                codeSummary.exitCode = code;
                codeSummary.details = errors[task][step][code].errors[0].details;
                codeSummary.type = errors[task][step][code].errors[0].type;
                codeSummary.jobs = errors[task][step][code].jobs;
                codeSummary.runLumis = getRunLumiRange(errors[task][step][code].runs)
                codeSummary.input = errors[task][step][code].input;
                summary[task][step][i] = codeSummary;
                i++;
                /*
                summary[task].totalJobs += codeSummary.jobs;
                summary[task].totalFiles += codeSummary.input.length;
                summary.totalJobs += codeSummary.jobs;
                summary.totalFiles += codeSummary.input.length;
                */
            };
        };
        //summary.totalFailureTime += summary[task].failureTime;
    };
    
    return summary;
}

function sortNumber(a, b) {
    return a - b;
}
function getRunLumiRange(runLumiInfo){
    var runSummary = {};
    for (var runNumber in runLumiInfo) {
        runSummary[runNumber] = {};
        runSummary[runNumber].lumiRange = [];
        //sort within but returns itself.
        var sorted = runLumiInfo[runNumber].sort(sortNumber);
        var start = sorted[0], end = sorted[0];
        var k = 0;
        for (var i = 0; i < runLumiInfo[runNumber].length; i++) {
            if (end + 1 < sorted[i]) {
                runSummary[runNumber].lumiRange[k] = [start, end];
                start = sorted[i];
                k++;
            }
            end = sorted[i];
        };
        runSummary[runNumber].lumiRange[k] = [start, end];
    }
    return runSummary;
}