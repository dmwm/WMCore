// handles ajax call other than couchdb
WMStats.namespace("Ajax");

WMStats.Ajax = (function($){
    var reqMgrFuncs = {
        putRequest: function(requestArgs) {
        	var uri = "/reqmgr2/data/request";
        	var verb = "POST"; 
            $.ajax(uri, 
                   {type: verb,
                    //accept: {json: "application/json"},
                    //contentType: "application/json",
                    headers: {"Accept": "application/json",
                              "Content-Type": "application/json"},
                    data: JSON.stringify(requestArgs),
                    processData: false,
                    success: function(data, textStatus, jqXHR) {
                    	 	var reqInfo = {};
                    	 	reqInfo.name = data.result[0].request;
                            reqInfo.reqmgr2Only = true;
                            $(WMStats.Globals.Event).triggerHandler(WMStats.CustomEvents.RESUBMISSION_SUCCESS, reqInfo);
                            },
                    error: function(jqXHR, textStatus, errorThrown){
                            console.log("call fails, response: " + jqXHR.responseText);
                        }
                    });
              },
   };
    
    var phedexFuncs = {
        getPFN: function(location, lfn) {
            $.ajax("/phedex/datasvc/json/prod/lfn2pfn", 
                   {type: 'GET',
                    //accept: {json: "application/json"},
                    //contentType: "application/json",
                    headers: {"Accept": "application/json"},
                    data: {node: location,
                           protocol: "srmv2",
                           lfn: lfn},
                    processData: false,
                    success: function(data, textStatus, jqXHR) {
                                /*
                                 * returned data format
                                 * 
                                 * {"phedex":{"mapping":[{"protocol":"srmv2",
                                 *                       "custodial":null,"destination":null,"space_token":null,
                                 *                       "node":"T2_US_MIT",
                                 *                       "lfn":"/store/unmerged/logs/prod/2013/4/4/jen_a_ACDC234Pro_Winter532012DMETParked_FNALPrio4_537p6_130403_042115_5913/DataProcessing/DataProcessingMergeRECOoutput/skim_2012D_METParked/0001/1/00710a8c-9c5e-11e2-9408-003048f02d38-1-1-logArchive.tar.gz",
                                 *                       "pfn":"srm://se01.cmsaf.mit.edu:8443/srm/v2/server?SFN=/mnt/hadoop/cms/store/unmerged/logs/prod/2013/4/4/jen_a_ACDC234Pro_Winter532012DMETParked_FNALPrio4_537p6_130403_042115_5913/DataProcessing/DataProcessingMergeRECOoutput/skim_2012D_METParked/0001/1/00710a8c-9c5e-11e2-9408-003048f02d38-1-1-logArchive.tar.gz"
                                 *                      }],
                                 *            "request_timestamp":1366230636.06684,"instance":"prod",
                                 *            "request_url":"http://cmsweb.cern.ch:7001/phedex/datasvc/json/prod/lfn2pfn",
                                 *            "request_version":"2.3.15-comp","request_call":"lfn2pfn","call_time":0.02142,
                                 *            "request_date":"2013-04-17 20:30:36 UTC"}}
                                 */
                                var mappedData = data.phedex.mapping;
                                var pfns = [];
                                for (var i in mappedData) {
                                    pfns.push(mappedData[i].pfn);
                                }
                                $(WMStats.Globals.Event).triggerHandler(WMStats.CustomEvents.PHEDEX_PFN_SUCCESS, pfns);
                            },
                    error: function(jqXHR, textStatus, errorThrown){
                            console.log("call fails, response: " + jqXHR.responseText);
                        }
                    });
        }
    };
    
    return {
        requestMgr: reqMgrFuncs,
        phedex: phedexFuncs
        };
})(jQuery);
