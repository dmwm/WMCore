WMStats.namespace("RunModel")

WMStats.RunModel = (function() {
    /*
     * create campaign table view.
     */
    // campaign summary data
    var _data = null;
    // div id for the view
    var _containerDiv = null;
    // default couchdb view name to get the campaign dat
    var _viewName = 'runStatus';
    // default option
    var _options = {'reduce': true, 'group_level':1, 'descending':true};

    function _get(obj, val) {
        if (obj) {
            return obj;
        } else {
            return val;
        } 
    }
    
    // jquery datatable config
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "campaign", "sTitle": "campaign"},               
            { "mDataProp": "new", "sTitle": "new", "sDefaultContent": 0},
            { "mDataProp": "assignment-approved", "sTitle": "approved", 
                           "sDefaultContent": 0},
            { "mDataProp": "assigned", "sTitle": "assigned", 
                           "sDefaultContent": 0},
            { "mDataProp": "ops-hold", "sTitle": "ops hold", 
                           "sDefaultContent": 0},
            { "mDataProp": "negotiating", "sTitle": "negotiating", 
                           "sDefaultContent": 0},
            { "mDataProp": "acquired", "sTitle": "acquired", 
                           "sDefaultContent": 0},
            { "mDataProp": "running", "sTitle": "running", "sDefaultContent": 0},
            { "sTitle": "failed", "sDefaultContent": 0, 
              "fnRender": function ( o, val ) {
                              return (_get(o.aData['failed'], 0) + 
                                     _get(o.aData['epic-FAILED'], 0));
                          },
            },
            { "mDataProp": "completed", "sTitle": "completed", 
                           "sDefaultContent": 0},
            { "mDataProp": "closed-out", "sTitle": "closed-out",
                           "sDefaultContent": 0},
            { "mDataProp": "announced", "sTitle": "announced",
                           "sDefaultContent": 0},
            { "mDataProp": "aborted", "sTitle": "aborted", "sDefaultContent": 0},
            { "mDataProp": "rejected", "sTitle": "rejected", "sDefaultContent": 0},
            { "mDataProp": "deleted", "sTitle": "deleted", "sDefaultContent": 0}
        ]
    }
    
    function getData() {
        return _data;
    }

    function setCampaignData(data) {
        //TODO sync with group level
        //This is base on the group_level ["campaign", "team", type]
        var baseColumns = ["campaign"];
        var rows =[]
        for (var i in data) {
            var tableRow = data[i].value;
            for (var j = 0; j < baseColumns.length; j ++) {
                tableRow[baseColumns[j]] = data[i].key[j];
            }
            rows.push(tableRow)
        }
        _data = rows;
        return rows
    }
    
    function createCampaignTable(data) {
        setCampaignData(data.rows);
        tableConfig.aaData = _data;
        var selector =  _containerDiv + " table";
        return WMStats.Table(tableConfig).create(selector)
    }
    
   function createTable(selector) {
        _containerDiv = selector;
        $(selector).html( '<table cellpadding="0" cellspacing="0" border="0" class="display"></table>' );
        WMStats.Couch.view(_viewName, _options, createCampaignTable, WMStats.Globals.AJAX_LOADING_STATUS)
    }
    
    return {'getData': getData, 'createTable': createTable};
     
})();