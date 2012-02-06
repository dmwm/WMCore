WMStats.namespace("CampaignView")

WMStats.CampaignView = (function() {
    
    var _data = null;
    var _containerDiv = null;
    var _url = WMStats.Globals.couchDBViewPath + 'campaign-status';
    var _options = {'reduce': true, 'group_level':1, 'descending':true};
    var _tableID = "campaignTable";
 
    function _get(obj, val) {
        if (obj) {
            return obj;
        } else {
            return val;
        } 
    }
    
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "campaign", "sTitle": "campaign"},               
            //{ "sTitle": "team"},
            //{ "sTitle": "type"},
            { "mDataProp": "new", "sTitle": "new", "sDefaultContent": 0},
            { "mDataProp": "testing-approved", "sTitle": "testing approved", 
                           "bVisible": false, "sDefaultContent": 0 },
            { "mDataProp": "testing", "sTitle": "testing", "sDefaultContent": 0, 
                           "bVisible": false },
            { "mDataProp": "tested", "sTitle": "tested", "bVisible": false,   
                           "sDefaultContent": 0 },
            { "mDataProp": "test-failed", "sTitle": "test failed", 
                           "bVisible": false, "sDefaultContent": 0 },
            { "mDataProp": "assignment-approved", "sTitle": "approved", 
                           "sDefaultContent": 0 },
            { "mDataProp": "assigned", "sTitle": "assigned", 
                           "sDefaultContent": 0 },
            { "mDataProp": "ops-hold", "sTitle": "ops hold", "sDefaultContent": 0 },
            { "mDataProp": "negotiating", "sTitle": "negotiating", "sDefaultContent": 0 },
            { "mDataProp": "acquired", "sTitle": "acquired", "sDefaultContent": 0 },
            { "mDataProp": "running", "sTitle": "running", "sDefaultContent": 0 },
            { "mDataProp": "failed", "sTitle": "failed", "sDefaultContent": 0, 
                           "bVisible": false},
            { "mDataProp": "epic-FAILED", "sTitle": "epic FAILED", 
                           "sDefaultContent": 0, "bVisible": false},
            { "sTitle": "failed", "sDefaultContent": 0, 
                        "fnRender": function ( o, val ) {
                                        return _get(o.aData['failed'], 0) + _get(o.aData['epic-Failed'], 0);
                                    }},
            { "mDataProp": "completed", "sTitle": "completed", "sDefaultContent": 0 },
            { "mDataProp": "closed-out", "sTitle": "closed-out", "sDefaultContent": 0 },
            { "mDataProp": "announced", "sTitle": "announced", "sDefaultContent": 0 },
            { "mDataProp": "aborted", "sTitle": "aborted", "sDefaultContent": 0 },
            { "mDataProp": "rejected", "sTitle": "rejected", "sDefaultContent": 0 }
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
            tableRow = data[i].value;
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
        var selector =  _containerDiv + " table#" + _tableID;
        return WMStats.Table(tableConfig).create(selector)
    }
    
   function createTable(selector) {
        _containerDiv = selector;
        $(selector).html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="'+ _tableID + '"></table>' );
        $.get(_url, _options, createCampaignTable, 'json')
    }
    
    return {'getData': getData, 'createTable': createTable};
     
})();