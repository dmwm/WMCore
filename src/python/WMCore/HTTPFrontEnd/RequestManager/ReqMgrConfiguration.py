import os, re, socket
from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration

__all__ = []

basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def reqMgrConfig(
    componentDir =  basedir + "/var",
    installation = os.path.join(getWMBASE(), 'src'),
    port = 8240,
    user = None,
    reqMgrHost = "http://%s:%d" % (socket.gethostname().lower(), 8240),
    proxyBase = None,
    couchurl = os.getenv("COUCHURL"),
    sitedb = 'https://cmsweb.cern.ch/sitedb/json/index/CEtoCMSName?name',
    yuiroot = 'http://yui.yahooapis.com/2.8.0r4',
    configCouchDB = 'reqmgr_config_cache',
    workloadCouchDB = 'reqmgr_workload_cache',
    connectURL = None,
    startup = "Root.py"):

    config = Configuration()
    reqMgrHtml = os.path.join(installation, 'html/RequestManager')
    reqMgrTemplates = os.path.join(installation, 'templates/WMCore/WebTools/RequestManager')
    requestOverviewHtml = os.path.join(installation, 'html')
    requestOverviewTemplates = os.path.join(installation, 'templates/WMCore/WebTools')
    requestOverviewJavascript = os.path.join(installation, 'javascript')
   

    if startup == "Root.py":
        config.component_("Webtools")
        config.Webtools.host = '0.0.0.0'
        config.Webtools.port = port
        config.Webtools.application = "reqmgr"
        if(proxyBase):
            config.Webtools.proxy_base = proxy_base
        config.Webtools.environment = 'production'
        config.component_('reqmgr')
        from ReqMgrSecrets import connectUrl
        config.section_("CoreDatabase")
        #read from Secrets file
        config.CoreDatabase.connectUrl = connectUrl
        config.reqmgr.section_('database')
        config.reqmgr.database.connectUrl = connectUrl
    else:
        config.webapp_("reqmgr")
        config.reqmgr.Webtools.host = '0.0.0.0'
        config.reqmgr.Webtools.port = port
        config.reqmgr.Webtools.environment = 'devel'
        config.reqmgr.database.connectUrl = connectURL
        
    config.reqmgr.componentDir = componentDir
    config.reqmgr.templates = reqMgrTemplates
    config.reqmgr.html = reqMgrHtml
    config.reqmgr.admin = 'cms-service-webtools@cern.ch'
    config.reqmgr.title = 'CMS Request Manager'
    config.reqmgr.description = 'CMS Request Manager'
    config.reqmgr.couchUrl = couchurl
    config.reqmgr.configDBName = configCouchDB
    config.reqmgr.workloadDBName = workloadCouchDB
    config.reqmgr.security_roles = ['Admin', 'Developer', 'Data Manager']
    config.reqmgr.yuiroot = yuiroot

    views = config.reqmgr.section_('views')
    active = views.section_('active')

    active.section_('view')
    active.view.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrBrowser'

    active.section_('admin')
    active.admin.object = 'WMCore.HTTPFrontEnd.RequestManager.Admin'

    active.section_('approve')
    active.approve.object = 'WMCore.HTTPFrontEnd.RequestManager.Approve'

    active.section_('assign')
    active.assign.object = 'WMCore.HTTPFrontEnd.RequestManager.Assign'
    active.assign.sitedb = sitedb

    active.section_('reqMgr')
    active.reqMgr.section_('model')
    active.reqMgr.section_('formatter') 
    active.reqMgr.object = 'WMCore.WebTools.RESTApi'
    active.reqMgr.model.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel'
    active.reqMgr.default_expires = 0 # no caching
    active.reqMgr.formatter.object = 'WMCore.WebTools.RESTFormatter'

    active.section_('create')
    active.create.object = 'WMCore.HTTPFrontEnd.RequestManager.WebRequestSchema'
    active.create.requestor = user
    active.create.cmsswDefaultVersion = 'CMSSW_3_5_8'

    active.section_('RequestOverview')
    active.RequestOverview.object = 'WMCore.HTTPFrontEnd.RequestManager.RequestOverview'
    active.RequestOverview.templates = requestOverviewTemplates
    active.RequestOverview.javascript = requestOverviewJavascript
    active.RequestOverview.html = requestOverviewHtml

    return config
