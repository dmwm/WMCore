import socket
"""
Defines default config values for errorhandler specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 1.3 2010/08/09 21:43:18 rpw Exp $"
__version__ = "$Revision: 1.3 $"

import os

from WMCore.Configuration import Configuration

cmsswInstallation =  '/uscmst1/prod/sw/cms'

config = Configuration()
#connectUrl = "mysql://rpw@localhost/reqmgr_rpw"
connectUrl = "oracle://rpw:PASSWORD@cmscald"
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
config.ReqMgr.Webtools.host = basehost
config.ReqMgr.Webtools.port = port
config.ReqMgr.database.connectUrl = connectUrl
#config.ReqMgr.database.socket = config.CoreDatabase.socket
config.ReqMgr.templates = os.environ['WTBASE'] + '/src/templates/WMCore/WebTools'
config.ReqMgr.admin = 'rickw@caltech.edu'
config.ReqMgr.title = 'CMS Request Manager'
config.ReqMgr.description = 'CMS Request manager'
config.ReqMgr.couchURL = os.environ['COUCHURL']
config.ReqMgr.couchDBName = 'wmagent_config_cache'
config.ReqMgr.workloadCouchDB = 'wmagent_spec_cache'

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

active.section_('reqMgrBrowser')
active.reqMgrBrowser.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrBrowser'
active.reqMgrBrowser.reqMgrHost = reqMgrHost
active.reqMgrBrowser.configCacheUrl = config.ReqMgr.couchURL
active.reqMgrBrowser.configDBName =  config.ReqMgr.couchDBName

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

active.reqMgr.section_('model')
active.reqMgr.model.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel'
active.reqMgr.model.reqMgrHost = reqMgrHost
active.reqMgr.model.couchUrl = config.ReqMgr.couchURL
active.reqMgr.model.workloadCouchDB = config.ReqMgr.workloadCouchDB
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
active.WebRequestSchema.configCacheUrl = config.ReqMgr.couchURL
active.WebRequestSchema.configCacheDBName = "wmagent_config_cache"
active.WebRequestSchema.templates = config.ReqMgr.templates
active.WebRequestSchema.componentDir = config.ReqMgr.componentDir
