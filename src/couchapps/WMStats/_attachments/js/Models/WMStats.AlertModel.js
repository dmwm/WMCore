WMStats.namespace("AlertModel");

WMStats.AlertModel = new WMStats._ModelBase('cooledoffRequests', 
                                          {"group_level": 1, "reduce": true}, 
                                          WMStats.Alerts);
WMStats.AlertModel.setTrigger("alertReady");
