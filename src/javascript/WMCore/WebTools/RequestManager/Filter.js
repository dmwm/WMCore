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
                req = req.toLowerCase();
                for (i = 0, l = data.length; i < l; ++i) {
                    if (!data[i].status.toLowerCase().indexOf(req)) {
                        filtered.push(data[i]);
                    }
                }
                res.results = filtered;
            }

            return res;
      };
 
    var dataTable = new YAHOO.widget.DataTable(tableInfo.divID, tableInfo.cols, 
	                                           dataSource, tableInfo.conf)
    
	var filterTimeout = null;
    var updateFilter = function(){
		// Reset timeout
		filterTimeout = null;
		
		// Get filtered data
		dataSource.sendRequest(YAHOO.util.Dom.get(filterDiv).value, {
			success: dataTable.onDataReturnInitializeTable,
			failure: dataTable.onDataReturnInitializeTable,
			scope: dataTable
		});
	};
	
    YAHOO.util.Event.on(filterDiv,'keyup',function (e) {
        clearTimeout(filterTimeout);
        setTimeout(updateFilter,600);
    });
};	