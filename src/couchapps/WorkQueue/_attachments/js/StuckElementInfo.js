WQ.namespace("StuckElementInfo")

WQ.StuckElementInfo.elementTable = function(args) {

    var dateFormatter = function(elCell, oRecord, oColumn, oData) {

        var oDate = new Date(oData*1000);
        //for the formatting check
        // http://developer.yahoo.com/yui/docs/YAHOO.util.Date.html#method_format
        var str = YAHOO.util.Date.format(oDate, { format:"%D %T"});
        elCell.innerHTML = str;
    }

    var siteFormatter = function(elCell, oRecord, oColumn, oData) {
    	var label = ""
        if (oData) {
            for (a in oData) {
            	label += a + " : [" + oData[a].join() + "], ";
            }
        }
        elCell.innerHTML = label

    };

    var listFormatter= function(elCell, oRecord, oColumn, oData) {
    	elCell.innerHTML = oData.join()
    };

    var dataSchema = {
        fields: [
        	     {key: "reason"},
        		 {key: "RequestName", label: "Request Name"},
                 {key: "SiteWhitelist", formatter: listFormatter},
                 {key: "SiteBlacklist", formatter: listFormatter},
                 {key: "Inputs", formatter: siteFormatter},
                 {key: "PileupData", formatter: siteFormatter},
                 {key: "ParentData", formatter: siteFormatter},
                 {key: "Priority"},
                 {key: "TeamName", label: "Team"},
                 {key: "InsertTime", label: "Insert Time", formatter:dateFormatter},
                 {key: "UpdateTime", label: "Update Time", formatter:dateFormatter},
                 {key: "id"}
                ]
        };

    //workqueue database name is hardcoded, need to change to get from config
    var dataUrl = "stuckElementsInfo/";
    var dataSource = WQ.createDataSource(dataUrl, dataSchema);

    var tableConfig = WQ.createDefaultTableConfig();

    tableConfig.paginator = new YAHOO.widget.Paginator({rowsPerPage : 50});

    var dataTable = WQ.createDataTable(args.divID, dataSource,
                         WQ.createDefaultTableDef(dataSchema.fields),
                         tableConfig, 600000);
}