WQ.namespace("ElementInfoByWorkflow")

WQ.ElementInfoByWorkflow.elementTable = function(args) {

    var agentName = function(elCell, oRecord, oColumn, sData) {
			var host;
            if (!sData) {
                host = sData;
            } else {
                host = sData.split("//")[1]
                host = host.split(":")[0]
            }
            elCell.innerHTML = host;
        };

    var elementUrl = function(elCell, oRecord, oColumn, sData) {
        elCell.innerHTML = "<a href='" + 'element/' + sData +
        "' target='_blank'>" + sData + "</a>";
    }

    var percentFormat = function(elCell, oRecord, oColumn, sData) {
            if (!sData) {
                percent = 0;
            } else {
                percent = sData
            }
            elCell.innerHTML =  sData + "%";
        };

    var dateFormatter = function(elCell, oRecord, oColumn, oData) {

        var oDate = new Date(oData*1000);
        //for the formatting check
        // http://developer.yahoo.com/yui/docs/YAHOO.util.Date.html#method_format
        var str = YAHOO.util.Date.format(oDate, { format:"%D %T"});
        elCell.innerHTML = str;
    }

    var inputFormatter = function(elCell, oRecord, oColumn, oData) {

        if (oData) {
            for (a in oData) {
                //If there are more than one input data add
                elCell.innerHTML = a;
            }
        }else {
            elCell.innerHTML = "No Input";
        }

    }

    var siteFormatter= function(elCell, oRecord, oColumn, oData) {
        var siteInfo = oRecord.getData("Inputs");
        if (siteInfo) {
            for (a in siteInfo) {
                //If there are more than one input data add
                elCell.innerHTML = siteInfo[a];
            }
        }else {
            elCell.innerHTML = "No sites"
        };
    };

    var dataSchema = {
        fields: [{key: "Id", label: "Id", formatter : elementUrl},
                 {key: "RequestName", label: "Request Name"},
                 {key: "TaskName", label: "Task Name"},
                 {key: "Inputs", formatter: inputFormatter},
                 {key: "Status"},
                 {key: "ChildQueueUrl", label:"Child Agent", formatter: agentName},
                 {key: "Priority"},
                 {key: "Jobs", label: "jobs"},
                 {key: "TeamName", label: "Team"},
                 {key: "PercentComplete", label: "Complete", formatter:percentFormat},
                 {key: "PercentSuccess", label: "Success", formatter:percentFormat},
                 {key: "InsertTime", label: "Insert Time", formatter:dateFormatter},
                 {key: "UpdateTime", label: "Update Time", formatter:dateFormatter},
                 {key: "Sites", formatter:siteFormatter}
                 //,{key: "error"},
                 //{key: "reason"}
                ]
        };

    //This makes this javascript not reusable but solves the path issue on
    //different deployment (using proxy, rewrite rules.
    var dataUrl = "elementsInfo?request=" + args.workflow
    var dataSource = WQ.createDataSource(dataUrl, dataSchema)

    var tableConfig = WQ.createDefaultTableConfig();

    tableConfig.paginator = new YAHOO.widget.Paginator({rowsPerPage : 25});

    var dataTable = WQ.createDataTable(args.divID, dataSource,
                         WQ.createDefaultTableDef(dataSchema.fields),
                         tableConfig, 600000);
}