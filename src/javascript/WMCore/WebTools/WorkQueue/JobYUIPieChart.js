var jobChart = function(divID) {
    
	YAHOO.widget.Chart.SWFURL = "http://yui.yahooapis.com/2.8.1/build/charts/assets/charts.swf";
    //--- data

    var dataSchema = {
        fields: [{key: "status"}, {key: "jobs"}]
        };

    var dataUrl = "/workqueue/jobstatusstat"

    var dataSource = WMCore.WebTools.createDataSource(dataUrl, dataSchema)

    //--- chart

    var mychart = new YAHOO.widget.PieChart(divID, dataSource,
    {
        dataField: "jobs",
        categoryField: "status",
        style:
        {
            padding: 20,
            legend:
            {
                display: "right",
                padding: 10,
                spacing: 5,
                font:
                {
                    family: "Arial",
                    size: 12
                }
            }
        },
        //only needed for flash player express install
        expressInstall: "assets/expressinstall.swf"
    });
};