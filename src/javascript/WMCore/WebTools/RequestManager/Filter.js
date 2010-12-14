WMCore.namespace("WebTools.RequestManager.Filter")

WMCore.WebTools.RequestManager.Filter.addLocalFilter = function(
                                filterDiv, data, dataSchema, tableInfo) {    
    
	var dataSource = WMCore.WebTools.createDataSource(data, dataSchema, "Local");
        
    dataSource.doBeforeCallback = function (req,raw,res,cb) {
            // This is the filter function
            var data     = res.results || [],
                filtered = [],
                i,l;

            if (req) {
				reqA = req.split('&');
				for (i = 0, l = data.length; i < l; ++i) {
					filterFlag0 = true;
					filterFlag1 = true;
					filterFlag2 = true;
					if (reqA[0]) {
						if (!data[i].request_name.toLowerCase().indexOf(reqA[0].toLowerCase())) {
							filterFlag0 = true;
						} else {
							filterFlag0 = false;
						}
					}
					if (reqA[1]) {
						if (!data[i].status.toLowerCase().indexOf(reqA[1].toLowerCase())) {
							filterFlag1 = true;
						} else {
							filterFlag1 = false;
						}
					}
					if (reqA[2]) {
						if (!data[i].type.toLowerCase().indexOf(reqA[2].toLowerCase())) {
							filterFlag2 = true;
						} else {
							filterFlag2 = false;
						}
					}
					if (filterFlag0 && filterFlag1 && filterFlag2) {
						filtered.push(data[i]);
					}
                }
                res.results = filtered;
            }

            return res;
      };
 
    var dataTable = new YAHOO.widget.DataTable(tableInfo.divID, tableInfo.cols, 
	                                           dataSource, tableInfo.conf);
    
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
			failure: dataTable.onDataReturnReplaceRows,
			scope: dataTable
		});
	};
	
    YAHOO.util.Event.on(filterDiv,'keyup',function (e) {
        clearTimeout(filterTimeout);
        setTimeout(updateFilter,600);
    });
};	