WMCore.namespace("GlobalMonitor.Filter")

WMCore.GlobalMonitor.Filter.addLocalFilter = function(filterDiv, data,
                                             dataSchema, tableInfo, perPageID){

    var dataSource = WMCore.createDataSource(data, dataSchema, "Local");

    dataSource.doBeforeCallback = function(req, raw, res, cb){
        // This is the filter function
        var data = res.results || [], filtered = [], i, l;

        if (req) {
            reqA = req.split('&');
            for (var i = 0, l = data.length; i < l; ++i) {
                filterFlag0 = true;
                filterFlag1 = true;
                filterFlag2 = true;
                filterFlag3 = true;
                filterFlag4 = true;
                
                if (reqA[0]) {
                    if (data[i].request_name.toLowerCase().indexOf(reqA[0].toLowerCase()) != -1) {
                        filterFlag0 = true;
                    }
                    else {
                        filterFlag0 = false;
                    }
                }
                if (reqA[1]) {
                    if (data[i].status.toLowerCase().indexOf(reqA[1].toLowerCase()) != -1) {
                        filterFlag1 = true;
                    }
                    else {
                        filterFlag1 = false;
                    }
                }
                if (reqA[2]) {
                    if (data[i].type.toLowerCase().indexOf(reqA[2].toLowerCase()) != -1) {
                        filterFlag2 = true;
                    }
                    else {
                        filterFlag2 = false;
                    }
                }
                if (reqA[3]) {
                    if (data[i].site_whitelist == null) {
                        filterFlag3 = false;
                    } else {
                        filterFlag3 = !data[i].site_whitelist.every(function(ele) {
                            return ele.toLowerCase().indexOf(reqA[3].toLowerCase()) == -1
                        })
                    }
                }
                if (reqA[4]) {
                    if (data[i].local_queue == null) {
                        filterFlag4 = false;
                    } else {
                        filterFlag4 = !data[i].local_queue.every(function(ele) {
                            return ele.toLowerCase().indexOf(reqA[4].toLowerCase()) == -1
                        })
                    }
                }
                if (filterFlag0 && filterFlag1 && filterFlag2 && filterFlag3 && filterFlag4) {
                    filtered.push(data[i]);
                }
            }
            res.results = filtered;
            tableInfo.conf.totalRecords = filtered.length;
        }

        return res;
    };

    var dataTable = new YAHOO.widget.DataTable(tableInfo.divID, tableInfo.cols,
                                               dataSource, tableInfo.conf);

    var requestString = ""
    for (filter in filterDiv) {
        requestString += YAHOO.util.Dom.get(filterDiv[filter]).value;
        requestString += "&";
    };

    // Get filtered data
    dataSource.sendRequest(requestString, {
        success: dataTable.onDataReturnReplaceRows,
        failure:  function(){
            YAHOO.log("Polling failure", "error");
        },
        scope: dataTable,
        arguments: dataTable.getState()
    });

    var filterTimeout = null;
    var updateFilter = function(){
        // Reset timeout
        filterTimeout = null;
        var requestString = ""
        for (filter in filterDiv) {
            requestString += YAHOO.util.Dom.get(filterDiv[filter]).value;
            requestString += "&";
        };

        // Get filtered data
        dataSource.sendRequest(requestString, {
            success: dataTable.onDataReturnReplaceRows,
            failure:  function(){
                YAHOO.log("Polling failure", "error");
            },
            scope: dataTable,
            arguments: dataTable.getState()
        });
    };

    YAHOO.util.Event.on(filterDiv, 'keyup', function(e){
        clearTimeout(filterTimeout);
        setTimeout(updateFilter, 600);
    });
    YAHOO.util.Event.on(perPageID, 'change', function(e){
        tableInfo.conf.paginator.setRowsPerPage(YAHOO.util.Dom.get(perPageID).value, false);
        // Get filtered data
    });
};
