#!/usr/bin/env python
"""
"""

import os

from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration

serverHostName = "HOST_NAME"
CRABInterfacePort = 8888

workDirectory = "/CRABInterface/worgkin/dir"

couchURL = "http://user:passwd@host:5984"
workloadCouchDB = "workloadCouchDB"

databaseUrl = "mysql://root@localhost/ReqMgrDB"
databaseSocket = "/path/mysql.sock"

yuiRoot = "http://yui.yahooapis.com/2.8.0r4"

config = Configuration()

config.section_("General")
config.General.workDir = workDirectory
config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = databaseUrl
config.CoreDatabase.socket = databaseSocket

config.webapp_("CRABInterface")
CRABInterfaceUrl = "http://%s:%s" % (serverHostName, CRABInterfacePort)

config.CRABInterface.componentDir = config.General.workDir + "/CRABInterface"
config.CRABInterface.Webtools.host = serverHostName
config.CRABInterface.Webtools.port = CRABInterfacePort
config.CRABInterface.templates =os.path.join(getWMBASE(),
                                       "src/templates/WMCore/WebTools")

config.CRABInterface.configCacheCouchURL = "YourConfigCacheUrl"
config.CRABInterface.configCacheCouchDB = "configCacheCouchDB-Name"

config.CRABInterface.agentDN = "/Your/Agent/DN.here/"
config.CRABInterface.SandBoxCache_endpoint = "USB-cache-endpoint"
config.CRABInterface.SandBoxCache_port  = "PORT"
config.CRABInterface.SandBoxCache_basepath ="/Path/if/Needed"


config.CRABInterface.admin = "admin@mail.address"
config.CRABInterface.title = "CRAB REST Interface"
config.CRABInterface.description = "rest interface for crab"
config.CRABInterface.instance = "Analysis WMAGENT"
config.CRABInterface.yuiroot = yuiRoot

config.CRABInterface.section_("security")
config.CRABInterface.security.dangerously_insecure = True

config.CRABInterface.section_('views')
config.CRABInterface.views.section_('active')
config.CRABInterface.views.active.section_('crab')
config.CRABInterface.views.active.crab.section_('model')
config.CRABInterface.views.active.crab.section_('formatter')
config.CRABInterface.views.active.crab.object = 'WMCore.WebTools.RESTApi'
config.CRABInterface.views.active.crab.templates =  os.path.join(getWMBASE(),
                                       "src/templates/WMCore/WebTools/")
config.CRABInterface.views.active.crab.model.couchUrl = couchURL
config.CRABInterface.views.active.crab.model.workloadCouchDB = workloadCouchDB
config.CRABInterface.views.active.crab.model.object = 'CRABRESTModel'
config.CRABInterface.views.active.crab.formatter.object = 'WMCore.WebTools.RESTFormatter'


