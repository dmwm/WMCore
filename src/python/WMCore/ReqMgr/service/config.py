"""
ReqMgr (ReqMgr2) service configuration file.

WMCore.REST infrastructure inspired by t0wmaservice.

""" 


from WMCore.Configuration import Configuration


class Config(Configuration):
    """
    ReqMgr main configuration class.
    Most ReqMgr configuration values should be defined here.
    Only bare necessary items passed from the deployment.
  
    """
  
    def __init__(self,
                 port=None,
                 couch_host=None,
                 num_threads=None,
                 key_file=None):
        """
        :arg integer port: Server port.
        :arg couchdb_host: Server where CouchDB database runs.
        :arg integer nthreads: Number of server threads to create.
        :arg str key_file: Location of wmcore security header authentication key.
        
        """
        Configuration.__init__(self)
        main = self.section_("main")
        srv = main.section_("server")
        srv.thread_pool = num_threads
        main.application = "ReqMgr"
        main.port = port
        main.index = "resthub"
    
        # TODO/NOTE:
        # defined in the ReqMgr1 configuration, HTTPFrontEnd/RequestManager/ReqMgrConfiguration.py
        # probably not necessary, to be fetched from SiteDB
        # config.reqmgr.security_roles = ['Admin', 'Developer', 'Data Manager', 'developer', 'admin', 'data-manager']        
        main.authz_defaults = {"role": None, "group": None, "site": None}
        sec = main.section_("tools").section_("cms_auth")
        sec.key_file = key_file
        
        app = self.section_("reqmgr")
        #app.admin = "cms-service-webtools@cern.ch"
        app.admin = "zdenek.maxa@cern.ch"
        app.description = "CMS data operations Request Manager."
        app.title = "CMS Request Manager (ReqMgr)"        
        
        views = self.section_("views")
        
        # redirector for the REST API implemented handlers
        resthub = views.section_("resthub")
        resthub.object = "WMCore.ReqMgr.service.hub.Hub"
        resthub.couch_host = couch_host
        resthub.sitedb_url = "https://cmsweb.cern.ch/sitedb/json/index/CEtoCMSName?name"
        resthub.couch_config_cache_db = "reqmgr_config_cache"
        resthub.couch_reqmgr_db = "reqmgr_workload_cache"
        resthub.couch_workload_summary_db = "workloadsummary"
        resthub.couch_wmstats_db = "wmstats"
        # number of past days since when to display requests in the default view
        resthub.default_view_requests_since_num_days = 30 # days
        """
        TODO:
        - add info on meta-data database in couch (request states, teams, etc)
        - add url to fetch users, roles, groups from sitedb
        - add information about caching and page expiration
        - active.create.cmsswDefaultVersion = 'CMSSW_5_2_5' should be configurable this way?
            (it's indeed used on the web GUI request create page)
        """
        resthub.tag_collector_url = "https://cmstags.cern.ch/tc/ReleasesXML/?anytype=1"
        
        # web user interface
        ui = views.section_("ui")
        ui.object = "WMCore.ReqMgr.webgui.frontpage.FrontPage"