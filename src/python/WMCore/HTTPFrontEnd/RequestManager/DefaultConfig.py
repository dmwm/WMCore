import os, re, socket
from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration
from ReqMgrSecrets import connectUrl
__all__ = []

BASEDIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIGDIR = os.path.normcase(os.path.abspath(__file__)).rsplit('/', 1)[0]

PORT = 8240
reqMgrHost = "http://127.0.0.1:%d" % PORT
HOST = socket.gethostname().lower()
COUCH = "http://localhost:5984"
if re.match(r"^vocms(?:10[67]|5[03])\.cern\.ch$", HOST):
  HOST = "cmsweb.cern.ch"
  COUCH = "http://vocms53.cern.ch:5984"
elif re.match(r"^vocms51\.cern\.ch$", HOST):
  HOST = "cmsweb-testbed.cern.ch"
elif 'fnal' in HOST:
  COUCH = "http://dmwmwriter:PASSWORD@cmssrv52.fnal.gov:5984"
TEMPLATES = os.path.join(getWMBASE(), 'src/templates/WMCore/WebTools')

config = Configuration()
config.component_('SecurityModule')
config.SecurityModule.key_file = BASEDIR+'/var/binkey'

config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = connectUrl

config.component_("Webtools")
config.Webtools.host = '0.0.0.0'
config.Webtools.port = PORT
config.Webtools.application = "ReqMgr"

config.component_('ReqMgr')
config.webapp_("ReqMgr")
config.ReqMgr.componentDir = BASEDIR + "/var"
config.ReqMgr.database.connectUrl = connectUrl
config.ReqMgr.templates = TEMPLATES
config.ReqMgr.admin = 'cms-service-webtools@cern.ch'
config.ReqMgr.title = 'CMS Request Manager'
config.ReqMgr.description = 'CMS Request manager'
config.ReqMgr.couchUrl = COUCH
config.ReqMgr.configDBName = 'wmagent_config_cache'

views = config.ReqMgr.section_('views')
active = views.section_('active')

active.section_('reqMgrBrowser')
active.reqMgrBrowser.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrBrowser'
active.reqMgrBrowser.reqMgrHost = reqMgrHost
active.reqMgrBrowser.couchUrl = COUCH
active.reqMgrBrowser.configDBName = config.ReqMgr.configDBName

active.section_('reqMgr')
active.reqMgr.section_('model')
active.reqMgr.section_('formatter') 
active.reqMgr.object = 'WMCore.WebTools.RESTApi'
active.reqMgr.model.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel'
active.reqMgr.model.reqMgrHost = reqMgrHost
active.reqMgr.model.couchUrl = COUCH
active.reqMgr.model.workloadCouchDB = 'wmagent_workload_cache'
active.reqMgr.default_expires = 0 # no caching
active.reqMgr.formatter.object = 'WMCore.WebTools.RESTFormatter'
active.reqMgr.formatter.templates = TEMPLATES

active.section_('WebRequestSchema')
active.WebRequestSchema.object = 'WMCore.HTTPFrontEnd.RequestManager.WebRequestSchema'
active.WebRequestSchema.requestor = 'rpw'
active.WebRequestSchema.reqMgrHost = reqMgrHost
active.WebRequestSchema.cmsswDefaultVersion = 'CMSSW_3_5_8'
active.WebRequestSchema.couchUrl = COUCH
active.WebRequestSchema.configCacheDBName = "wmagent_config_cache"
active.WebRequestSchema.templates = TEMPLATES

active.section_('RequestOverview')
active.RequestOverview.object = 'WMCore.HTTPFrontEnd.RequestManager.RequestOverview'
active.RequestOverview.templates = TEMPLATES
active.RequestOverview.javascript = os.path.join(getWMBASE(), 'src/javascript')
active.RequestOverview.html = os.path.join(getWMBASE(), 'src/html')

