import socket
"""
Defines default config values for errorhandler specific
parameters.
"""
__all__ = []



import os

from WMCore.Configuration import Configuration

cmsswInstallation =  '/uscmst1/prod/sw/cms'

config = Configuration()
#connectUrl = "mysql://rpw@localhost/reqmgr_rpw"
connectUrl = "oracle://rpw:changeme_2009@cmscald"
dbsock = '/var/lib/mysql/mysql.sock'
dbsock = ""
config.section_("CoreDatabase")
#config.CoreDatabase.dialect = 'mysql'
#config.CoreDatabase.socket = dbsock

config.CoreDatabase.connectUrl = connectUrl
#config.CoreDatabase.dbsock = dbsock
basehost = "cmssrv49.fnal.gov"
host = "http://"+basehost
port = 8585

config.component_('ReqMgr')
config.ReqMgr.templates = os.environ['WTBASE'] + '/src/templates/WMCore/WebTools'
config.ReqMgr.admin = 'rickw@caltech.edu'
config.ReqMgr.title = 'CMS Request Manager'
config.ReqMgr.description = 'CMS Request manager'

config.webapp_("ReqMgr")
config.ReqMgr.componentDir = "/home/rpw/ReqMgr"
config.ReqMgr.server.host = basehost
config.ReqMgr.server.port = port
config.ReqMgr.database.connectUrl = connectUrl
#config.ReqMgr.database.socket = config.CoreDatabase.socket
config.ReqMgr.templates = os.environ['WTBASE'] + '/src/templates/WMCore/WebTools'
config.ReqMgr.admin = 'rickw@caltech.edu'
config.ReqMgr.title = 'CMS Request Manager'
config.ReqMgr.description = 'CMS Request manager'
config.ReqMgr.couchURL = 'http://cmssrv52.fnal.gov:5984'


reqMgrHost = '%s:%s' % (host, port)

# FIXME only needed for running from ROot.py?

# This component has all the configuration of CherryPy
config.component_('Webtools')
config.Webtools.host = basehost
config.Webtools.port = port
# This is the application
config.Webtools.application = 'ReqMgr'
views = config.ReqMgr.section_('views')
active = views.section_('active')


views = config.ReqMgr.section_('views')
active = views.section_('active')

# download workflows
active.section_('download')
active.download.object = 'WMCore.HTTPFrontEnd.Downloader'
active.download.dir = config.ReqMgr.componentDir

active.section_('reqMgrBrowser')
active.reqMgrBrowser.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrBrowser'
active.reqMgrBrowser.reqMgrHost = reqMgrHost
active.reqMgrBrowser.workloadCache = active.download.dir
active.reqMgrBrowser.configCacheUrl = config.ReqMgr.couchURL

#active.section_('CmsDriverWebRequest')
#active.CmsDriverWebRequest.object = 'ReqMgr.RequestInterface.WWW.CmsDriverWebRequest'
#active.CmsDriverWebRequest.cmsswInstallation = cmsswInstallation
#active.CmsDriverWebRequest.cmsswDefaultVersion = 'CMSSW_2_2_3'
#active.CmsDriverWebRequest.configCacheDbUrl = configCache
#config.component_('SecurityModule')
#config.SecurityModule.oid_server = 'http://localhost:8400/'
#config.SecurityModule.app_url = 'https://cmsweb.cern.ch/myapp'
#config.SecurityModule.handler = 'WMCore.WebTools.OidDefaultHandler'
#config.SecurityModule.mount_point = 'auth'
#config.SecurityModule.session_name = 'SecurityModule'
#config.SecurityModule.store = 'filestore'
#config.SecurityModule.store_path = os.environ['WMCOREBASE'] + '/tmp/security-store'


active.section_('reqMgr')
active.reqMgr.object = 'WMCore.WebTools.RESTApi'
#active.reqMgr. = config.ReqMgr.database.connectUrl
#active.reqMgr.dialect = config.ReqMgr.dialect
#active.reqMgr.database = 'mysql://rpw@localhost/reqmgr_rpw?unix_socket=/var/lib/mysql/mysql.sock'

active.reqMgr.section_('model')
active.reqMgr.model.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel'
active.reqMgr.model.workloadCache = active.download.dir
active.reqMgr.model.reqMgrHost = reqMgrHost
# no caching
active.reqMgr.default_expires = 0
active.reqMgr.section_('formatter') 
active.reqMgr.formatter.object = 'WMCore.WebTools.RESTFormatter'
active.reqMgr.formatter.templates = config.ReqMgr.templates

active.section_('WebRequestSchema')
active.WebRequestSchema.object = 'WMCore.HTTPFrontEnd.RequestManager.WebRequestSchema'
active.WebRequestSchema.requestor = 'rpw'
active.WebRequestSchema.reqMgrHost = reqMgrHost
active.WebRequestSchema.cmsswInstallation = cmsswInstallation
active.WebRequestSchema.cmsswDefaultVersion = 'CMSSW_3_5_8'
active.WebRequestSchema.configCacheUrl = "http://USERNAME:PASSWORD@cmssrv52.fnal.gov:5984"
active.WebRequestSchema.configCacheDBName = "wmagent_config_cache"
active.WebRequestSchema.templates = config.ReqMgr.templates
active.WebRequestSchema.componentDir = config.ReqMgr.componentDir
