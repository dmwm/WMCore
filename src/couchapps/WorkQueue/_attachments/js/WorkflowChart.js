WQ.namespace("WorkflowChart")

WQ.WorkflowChart.jobChart = function(args){

    var dataSchema = {
        fields: [{
            key: "RequestName"
        }, {
            key: "Jobs"
        }]
    };

    var dataUrl = "/workqueue/_design/WorkQueue/_rewrite/workflowInfo/" + args.workflow

    var dataSource = WQ.createDataSource(dataUrl, dataSchema)

    //--- chart
    google.load("visualization", "1", {packages:["corechart"]});
    var chartDraw = function(oRequest, oParsedResponse, oPayload){
        // Create and populate the data table.

        var data = new google.visualization.DataTable();
        data.addColumn('string', 'RequestName');
        data.addColumn('number', 'Jobs');
        var dataSize = oParsedResponse.results.length
        data.addRows(dataSize);
        for (var i = 0; i < dataSize; i++) {
            data.setValue(i, 0, oParsedResponse.results[i].RequestName);
            data.setValue(i, 1, oParsedResponse.results[i].Jobs);
        };

        // Create and draw the visualization.
        new google.visualization.PieChart(document.getElementById(args.divID)).
            draw(data, {width: 450, height: 300, title: "Jobs By Workflow"});
      };

    var myCallback = {
        success: chartDraw,
        failure: function(){
            YAHOO.log("Polling failure", "error");
        },
        scope: this
    };
    var poll = function(){
        dataSource.sendRequest(null, myCallback);
        dataSource.setInterval(60000, null, myCallback);
    }
    poll();
    //google.setOnLoadCallback(poll)

}