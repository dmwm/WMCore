WMStats.namespace("T0");

//inherites from the WMStats.RequestTable
WMStats.T0.RequestTable = Object.create(WMStats.RequestTable);
(function() {
    var t0Table = WMStats.T0.RequestTable;
    var t0TableConfig = [
            { "mDataProp": "workflow", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatReqDetailUrl(o.aData.workflow);
                      },
              "bUseRendered": false
            },
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            }]
    t0Table.tableConfig = t0TableConfig.concat(t0TableConfig._defaultTableConfig)
    
})()

WMStats.T0.RequestTable.tableConfig = {

        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatReqDetailUrl(o.aData.workflow);
                      },
              "bUseRendered": false
            },
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "mDataProp": "requestor", "sTitle": "requestor"},
            { "mDataProp": "request_type", "sTitle": "type"},
            { "mDataProp": "inputdataset", "sTitle": "inputdataset",
                           "sDefaultContent": ""},
            { "mDataProp": "site_white_list", "sTitle": "site white list",
                           "sDefaultContent": "",
              "fnRender": function ( o, val ) {
                            if (o.aData.site_white_list.length > 1) {
                                return "mulitple sites";
                            } else {
                                return o.aData.site_white_list[0];
                            }
                          },
              "bUseRendered": false,
            },
            { "mDataProp": "priority", "sTitle": "priority", "sDefaultContent": 0},
            { "sDefaultContent": 0,
              "sTitle": "queued", 
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.queued.first", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.pending", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "running", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.running", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.failure.create", 0) + 
                                    _get(o.aData, "status.failure.submit", 0) + 
                                    _get(o.aData, "status.failure.exception", 0));
                          }
            },
            
            { "sDefaultContent": 0,
              "sTitle": "canceled", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.canceled", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "success",
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.success", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "cooloff", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.cooloff", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "prev cooloff",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.submitted.retry", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              return (_get(o.aData, "status.inWMBS",  0) / 
                                      _get(o.aData, 'total_jobs', 1) * 100 + '%');
                        }
            }
            
            //TODO add more data (consult dataops)
        ]
    }



(function() {
    
    var _initialView = 'requestByCampaignAndDate';
    var _options = {'include_docs': true};
    
    function _getOrDefault(baseObj, objList, val) {
        
        if (baseObj[objList[0]]) { 
            if (objList.length == 1) {
                return baseObj[objList[0]];
            } else {
                return _getOrDefault(baseObj[objList[0]], objList.slice(1), val);
            }
        } else {
            return val;
        } 
    }
    
    function _get(baseObj, objStr, val) {
        objList = objStr.split('.');
        return _getOrDefault(baseObj, objList, val); 
    }
    
    function formatReqDetailUrl(request) {
        return '<a href="' + WMStats.Globals.REQ_DETAIL_URL_PREFIX + encodeURIComponent(request) + '" target="_blank">' + request + '</a>';
    }
    
    function formatWorkloadSummarylUrl(request, status) {
        if (status == "completed" || status == "announced" ||
            status == "closed-out" || status == "deleted") {
            return '<a href="' + WMStats.Globals.WORKLOAD_SUMMARY_URL_PREFIX + encodeURIComponent(request) + '" target="_blank">' + status + '</a>';
        } else {
            return status;
        }
    }
    
    var _defaultTableConfig = {
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow",
              "fnRender": function ( o, val ) {
                            return formatReqDetailUrl(o.aData.workflow);
                      },
              "bUseRendered": false
            },
            { "mDataProp": function (source, type, val) { 
                              return source.request_status[source.request_status.length -1].status
                           }, "sTitle": "status",
              "fnRender": function ( o, val ) {
                            return formatWorkloadSummarylUrl(o.aData.workflow, 
                                o.aData.request_status[o.aData.request_status.length -1].status);
                          },
              "bUseRendered": false
            },
            { "mDataProp": "requestor", "sTitle": "requestor"},
            { "mDataProp": "request_type", "sTitle": "type"},
            { "mDataProp": "inputdataset", "sTitle": "inputdataset",
                           "sDefaultContent": ""},
            { "mDataProp": "site_white_list", "sTitle": "site white list",
                           "sDefaultContent": "",
              "fnRender": function ( o, val ) {
                            if (o.aData.site_white_list.length > 1) {
                                return "mulitple sites";
                            } else {
                                return o.aData.site_white_list[0];
                            }
                          },
              "bUseRendered": false,
            },
            { "mDataProp": "priority", "sTitle": "priority", "sDefaultContent": 0},
            { "sDefaultContent": 0,
              "sTitle": "queued", 
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.queued.first", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "pending", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.pending", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "running", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.running", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "failure",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.failure.create", 0) + 
                                    _get(o.aData, "status.failure.submit", 0) + 
                                    _get(o.aData, "status.failure.exception", 0));
                          }
            },
            
            { "sDefaultContent": 0,
              "sTitle": "canceled", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.canceled", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "success",
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.success", 0);
                          }},
            { "sDefaultContent": 0,
              "sTitle": "cooloff", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.cooloff", 0);
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "prev cooloff",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.submitted.retry", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            { "sDefaultContent": 0,
              "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              return (_get(o.aData, "status.inWMBS",  0) / 
                                      _get(o.aData, 'total_jobs', 1) * 100 + '%');
                        }
            }
            
            //TODO add more data (consult dataops)
        ]
    }
    
    var _defaultFilterConfig = {
        "aoColumns": [
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true},
            {type: "text", bRegex: true, bSmart: true}
        ]
    }

    function create(selector, data) {
         var tableConfig = {};
         var filterConfig = {};
         if (WMStats.Globals.Variant == 'tier1') {
             tableConfig = _defaultTableConfig;
             filterConfig = _defaultFilterConfig;
         } else if (WMStats.Globals.Variant == 'tier0') {
             tableConfig = _defaultTableConfig;
             filterConfig = _defaultFilterConfig;
         } else if (WMStats.Globals.Variant == 'tier0') {
             tableConfig = _defaultTableConfig;
             filterConfig = _defaultFilterConfig;
         }
         tableConfig.aaData = data;
         return WMStats.Table(tableConfig).create(selector, filterConfig)
    }
    
    var tier1Config = {"initView": 'requestByCampaignAndDate',
                       "initOption": {'include_docs': true},
                       "create": create
                      }

    var tier0Config = {"initView": 'requestByRunNumber',
                       "initOption": {'include_docs': true},
                       "create": create
                      }

    var analysisConfig = {"initView": 'requestByUser',
                       "initOption": {'include_docs': true},
                       "create": create
                       }
                      
    
    
    if (WMStats.Globals.Variant == 'tier1') return tier1Config;
    if (WMStats.Globals.Variant == 'tier0') return tier0Config;
    if (WMStats.Globals.Variant == 'analysis') return analysisConfig;
    //tier1Config is defualt config
    return tier1Config;
})();
    