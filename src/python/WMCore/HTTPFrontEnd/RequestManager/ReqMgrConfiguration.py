import os, re, socket
from WMCore.WMInit import getWMBASE
from WMCore.Configuration import Configuration

__all__ = []

import os

basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def reqMgrConfig(
    componentDir =  basedir + "/var",
    installation = os.environ["WMCORE_ROOT"],
    port = 8240,
    user = None,
    reqMgrHost = "http://%s:%d" % (socket.gethostname().lower(), 8240),
    proxyBase = None,
    couchurl = os.getenv("COUCHURL"),
    # SiteDB v1 API
    #sitedb = 'https://cmsweb.cern.ch/sitedb/json/index/CEtoCMSName?name',
    # SiteDB v2 API
    sitedb = "https://cmsweb.cern.ch/sitedb/data/prod/site-names",
    yuiroot = "/reqmgr/yuiserver/yui",
    configCouchDB = 'reqmgr_config_cache',
    workloadCouchDB = 'reqmgr_workload_cache',
    workloadSummaryCouchDB = "workloadsummary",
    wmstatCouchDB = "wmstats",
    acdcCouchDB = "acdcserver",
    connectURL = None,
    startup = "Root.py",
    addMonitor = True):

    config = Configuration()
    reqMgrHtml = os.path.join(installation, 'data/html/RequestManager')
    reqMgrTemplates = os.path.join(installation, 'data/templates/WMCore/WebTools/RequestManager')
    reqMgrJavascript = os.path.join(installation, 'data/javascript')
    globalOverviewTemplates = os.path.join(installation, 'data/templates/WMCore/WebTools/GlobalMonitor')
    globalOverviewJavascript = reqMgrJavascript
    globalOverviewHtml = os.path.join(installation, 'data/html')

    if startup == "Root.py":
        # CMS web mode of ReqMgr running
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
        # localhost, via wmcoreD ReqMgr running
        # startup = "wmcoreD"
        config.webapp_("reqmgr")
        config.reqmgr.Webtools.host = '0.0.0.0'
        config.reqmgr.Webtools.port = port
        config.reqmgr.Webtools.environment = 'devel'
        config.reqmgr.database.connectUrl = connectURL                

    config.reqmgr.componentDir = componentDir
    config.reqmgr.templates = reqMgrTemplates
    config.reqmgr.html = reqMgrHtml
    config.reqmgr.javascript = reqMgrJavascript
    config.reqmgr.admin = 'cms-service-webtools@cern.ch'
    config.reqmgr.title = 'CMS Request Manager'
    config.reqmgr.description = 'CMS Request Manager'
    config.reqmgr.couchUrl = couchurl
    config.reqmgr.configDBName = configCouchDB
    config.reqmgr.workloadDBName = workloadCouchDB
    config.reqmgr.wmstatDBName = wmstatCouchDB
    config.reqmgr.acdcDBName = acdcCouchDB
    config.reqmgr.security_roles = ['Admin', 'Developer', 'Data Manager', 'developer', 'admin', 'data-manager']
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
    active.section_('closeout')
    active.closeout.object = 'WMCore.HTTPFrontEnd.RequestManager.CloseOut'
    active.section_('announce')
    active.announce.object = 'WMCore.HTTPFrontEnd.RequestManager.Announce'

    active.section_('reqMgr')
    active.reqMgr.section_('model')
    active.reqMgr.section_('formatter')
    active.reqMgr.object = 'WMCore.WebTools.RESTApi'
    active.reqMgr.model.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel'
    active.reqMgr.default_expires = 0 # no caching
    active.reqMgr.formatter.object = 'WMCore.WebTools.RESTFormatter'
    active.reqMgr.templates = os.path.join(installation, 'data/templates/WMCore/WebTools')
    #deprecate the old interface
    active.section_('rest')
    active.rest.section_('model')
    active.rest.section_('formatter')
    active.rest.object = 'WMCore.WebTools.RESTApi'
    active.rest.model.object = 'WMCore.HTTPFrontEnd.RequestManager.ReqMgrRESTModel'
    active.rest.default_expires = 0 # no caching
    active.rest.formatter.object = 'WMCore.WebTools.RESTFormatter'
    active.rest.templates = os.path.join(installation, 'data/templates/WMCore/WebTools')

    active.section_('create')
    active.create.object = 'WMCore.HTTPFrontEnd.RequestManager.WebRequestSchema'
    active.create.requestor = user
    active.create.cmsswDefaultVersion = 'CMSSW_5_2_5'

    if addMonitor:
        active.section_('GlobalMonitor')
        active.GlobalMonitor.object = 'WMCore.HTTPFrontEnd.GlobalMonitor.GlobalMonitorPage'
        active.GlobalMonitor.templates = globalOverviewTemplates
        active.GlobalMonitor.javascript = globalOverviewJavascript
        active.GlobalMonitor.html = globalOverviewHtml
        active.GlobalMonitor.serviceLevel = 'RequestManager'

        active.section_('monitorSvc')
        active.monitorSvc.serviceURL = "%s/reqmgr/reqMgr" % reqMgrHost
        active.monitorSvc.serviceLevel = active.GlobalMonitor.serviceLevel
        active.monitorSvc.workloadSummaryCouchURL = "%s/%s" % (couchurl, workloadSummaryCouchDB)
        active.monitorSvc.section_('model')
        active.monitorSvc.section_('formatter')
        active.monitorSvc.object = 'WMCore.WebTools.RESTApi'
        active.monitorSvc.model.object = 'WMCore.HTTPFrontEnd.GlobalMonitor.GlobalMonitorRESTModel'
        active.monitorSvc.default_expires = 0 # no caching
        active.monitorSvc.formatter.object = 'WMCore.WebTools.RESTFormatter'
        active.monitorSvc.template = os.path.join(installation, 'data/templates/WMCore/WebTools')

    return config