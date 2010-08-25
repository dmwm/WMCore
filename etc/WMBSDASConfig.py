#!/usr/bin/env python
"""
_WMBSDASConfig_

Sample configuration for the WMBS DAS service.
"""
from WMCore.Configuration import Configuration
from os import environ

config = Configuration()

config.component_("Webtools")
config.Webtools.application = "WMBSMonitoring"
config.Webtools.host = "cmssrv52.fnal.gov"
config.Webtools.port = 8085

config.component_("WMBSMonitoring")
config.WMBSMonitoring.templates = environ["WMCOREBASE"] + '/src/templates/WMCore/WebTools'
config.WMBSMonitoring.admin = "sfoulkes@fnal.gov"
config.WMBSMonitoring.title = "WMBS Monitoring"
config.WMBSMonitoring.description = "Monitoring of a WMBS instance"
config.WMBSMonitoring.instance = "ReReco WMAGENT"
config.WMBSMonitoring.couchURL = "http://cmssrv52:5984/_utils/document.html?tier1_skimming/"

config.WMBSMonitoring.section_('views')
# These are all the active pages that Root.py should instantiate
active = config.WMBSMonitoring.views.section_('active')
wmbs = active.section_('wmbs')
# The class to load for this view/page
wmbs.object = 'WMCore.WebTools.RESTApi'
wmbs.templates = environ['WMCOREBASE'] + '/src/templates/WMCore/WebTools/'
wmbs.database = 'mysql://sfoulkes:@localhost/WMAgentDB_sfoulkes'
wmbs.dbsocket = '/opt/MySQL-5.1/var/lib/mysql/mysql.sock'

wmbs.section_('model')
wmbs.model.object = 'WMCore.HTTPFrontEnd.WMBS.WMBSRESTModel'
wmbs.section_('formatter')
wmbs.formatter.object = 'WMCore.WebTools.DASRESTFormatter'
