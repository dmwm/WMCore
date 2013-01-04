WMStats.namespace("CampaignTable");

WMStats.CampaignTable = function (data, containerDiv) {
    
    function _get(obj, val) {
        if (obj) {
            return obj;
        } else {
            return val;
        } 
    }
        // jquery datatable config
    var tableConfig = {
        "sScrollX": "",
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
    tableConfig.aaData = data.getData();
    
    var filterConfig = {};
    
    return WMStats.Table(tableConfig).create(containerDiv, filterConfig);
};
