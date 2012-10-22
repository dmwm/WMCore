from WMCore.Configuration import Configuration
from os import environ, path

config = Configuration()

config.component_('Webtools')
config.Webtools.application = 'WMStats'
config.component_('WMStats')

config.WMStats.reqmgrURL = "https://cmsweb.cern.ch/reqmgr/reqMgr/"
config.WMStats.globalQueueURL = "https://cmsweb.cern.ch/couchdb/workqueue/"
config.WMStats.couchURL = "http://localhost:5984/wmstats/"
config.WMStats.pollInterval = 600
config.WMStats.port = 19999
