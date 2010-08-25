#!/usr/bin/env python
"""
_WMBSDASConfig_

Sample configuration for the WMBS DAS service.
"""
from WMCore.Configuration import Configuration
from os import environ
import WMCore.WMInit

config = Configuration()

config.webapp_("WMBSMonitoring")
config.WMBSMonitoring.componentDir = "/home/sfoulkes/WMAgent/work/Monitoring"
config.WMBSMonitoring.server.host = "cmssrv18.fnal.gov"
config.WMBSMonitoring.server.port = 8087
config.WMBSMonitoring.database.socket = "/opt/MySQL.5.0/var/lib/mysql/mysql.sock"
config.WMBSMonitoring.database.connectUrl = "mysql://sfoulkes:@localhost/ProdAgentDB_sfoulkes"

config.WMBSMonitoring.templates = WMCore.WMInit.getWMBASE() + '/src/templates/WMCore/WebTools'
config.WMBSMonitoring.admin = "sfoulkes@fnal.gov"
config.WMBSMonitoring.title = "WMBS Monitoring"
config.WMBSMonitoring.description = "Monitoring of a WMBS instance"
config.WMBSMonitoring.instance = "ReReco WMAGENT"
config.WMBSMonitoring.couchURL = "http://cmssrv52:5984/_utils/document.html?tier1_skimming/"

config.WMBSMonitoring.section_('views')
config.WMBSMonitoring.views.section_('active')
config.WMBSMonitoring.views.active.section_('wmbs')
config.WMBSMonitoring.views.active.wmbs.section_('model')
config.WMBSMonitoring.views.active.wmbs.section_('formatter')
config.WMBSMonitoring.views.active.wmbs.object = 'WMCore.WebTools.RESTApi'
config.WMBSMonitoring.views.active.wmbs.templates = WMCore.WMInit.getWMBASE() + '/src/templates/WMCore/WebTools/'
config.WMBSMonitoring.views.active.wmbs.model.object = 'WMCore.HTTPFrontEnd.WMBS.WMBSRESTModel'
config.WMBSMonitoring.views.active.wmbs.formatter.object = 'WMCore.WebTools.DASRESTFormatter'
