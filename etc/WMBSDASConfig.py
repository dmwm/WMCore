#!/usr/bin/env python
"""
_WMBSDASConfig_

Sample configuration for the WMBS DAS service.
"""
from WMCore.Configuration import Configuration
from os import environ
import WMCore.WMInit

config = Configuration()

config.section_("Agent")
config.Agent.hostName = "cmssrv52.fnal.gov"
config.Agent.contact = "sfoulkes@fnal.gov"
config.Agent.teamName = "DMWM"
config.Agent.agentName = "ReRecoDOMINATOR"

config.section_("General")
config.General.workDir = "/home/sfoulkes/WMAgent/work"

config.section_("CoreDatabase")
config.CoreDatabase.socket = "/opt/MySQL.5.0/var/lib/mysql/mysql.sock"
config.CoreDatabase.connectUrl = "mysql://sfoulkes:@localhost/ProdAgentDB_sfoulkes"

config.component_("Webtools")
config.Webtools.application = "WMBSMonitoring"
config.Webtools.host = "cmssrv18.fnal.gov"
config.Webtools.port = 8087
config.Webtools.componentDir = config.General.workDir + "/Webtools"

config.component_("WMBSMonitoring")
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
config.WMBSMonitoring.views.active.wmbs.database = 'mysql://sfoulkes:@localhost/WMAgentDB_sfoulkes'
config.WMBSMonitoring.views.active.wmbs.dbsocket = '/opt/MySQL.5.0/var/lib/mysql/mysql.sock'
config.WMBSMonitoring.views.active.wmbs.model.object = 'WMCore.HTTPFrontEnd.WMBS.WMBSRESTModel'
config.WMBSMonitoring.views.active.wmbs.formatter.object = 'WMCore.WebTools.DASRESTFormatter'

