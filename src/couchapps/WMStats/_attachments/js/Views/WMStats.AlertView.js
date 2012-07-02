WMStats.namespace("AlertView")

WMStats.AlertView = new WMStats._ViewBase('cooledoffRequests', 
                                          {"group_level": 1, "reduce": true}, 
                                          WMStats.Alerts, WMStats.AlertTable);
