from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
wmstatsUrl = "https://cmsweb.cern.ch/couchdb/wmstats"
requestName = "franzoni_RVCMSSW_5_3_10_patch1ZElSkim2011A_130531_200832_244"
newState = "announced"
wmstatSvc = WMStatsWriter(wmstatUrl)
wmstatSvc.updateRequestStatus(requestName, newState)