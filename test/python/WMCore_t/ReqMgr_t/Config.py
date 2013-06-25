
"""
ReqMgr only configuration file.
Everything configurable in ReqMgr is defined here.

"""


from WMCore.Configuration import Configuration
from os import path

class Config(Configuration):

    def __init__(self, appport, couchurl, secure=True):
        Configuration.__init__(self)
        main = self.section_("main")
        srv = main.section_("server")
        srv.thread_pool = 30
        main.application = "reqmgr2"
        main.port = appport # main application port it listens on
        main.index = "restapihub"
        sec = main.section_("tools").section_("cms_auth")
        if secure:
            # Defaults to allow any CMS authenticated user. Write APIs should require
            # additional roles in SiteDB (i.e. "Admin" role for the "ReqMgr" group)
            main.authz_defaults = {"role": None, "group": None, "site": None}
            sec.key_file = "%s/auth/wmcore-auth/header-auth-key" % __file__.rsplit('/', 3)[0]
        else:
            sec.policy = "dangerously_insecure"
        
        # this is where the application will be mounted, where the REST API
        # is reachable and this features in CMS web frontend rewrite rules
        app = self.section_(main.application) # string containing "reqmgr2"
        app.admin = "cms-service-webtools@cern.ch"
        app.description = "CMS data operations Request Manager."
        app.title = "CMS Request Manager (ReqMgr)"
        
        views = self.section_("views")
        
        # redirector for the REST API implemented handlers
        restapihub = views.section_("restapihub")
        restapihub.object = "WMCore.ReqMgr.Service.RestApiHub.RestApiHub"
        # The couch host is defined during deployment time.
        restapihub.couch_host = couchurl
        # practical to have this kind of configuration values not in
        # service related RPM (difficult/impossible to change in CMS web
        # deployment) but in the deployment configuration for the service
        restapihub.sitedb_url = "https://cmsweb.cern.ch/sitedb/json/index/CEtoCMSName?name"
        # main ReqMgr CouchDB database containing all requests with spec files attached
        restapihub.couch_reqmgr_db = "reqmgr_workload_cache"
        # ReqMgr database containing groups, teams, software, etc
        restapihub.couch_reqmgr_aux_db = "reqmgr_auxiliary"
        # ConfigCache - database with configuration documents
        restapihub.couch_config_cache_db = "reqmgr_config_cache"
        restapihub.couch_workload_summary_db = "workloadsummary"
        restapihub.couch_wmstats_db = "wmstats"
        # number of past days since when to display requests in the default view
        restapihub.default_view_requests_since_num_days = 30 # days
        # resource to fetch CMS software versions and scramarch info from
        restapihub.tag_collector_url = "https://cmstags.cern.ch/tc/ReleasesXML/?anytype=1"
        # another source at TC, returns directly JSON, but strangely formatted (e.g.
        # keys are not present at easy item but defined in a dedicated item ...)
        # https://cmstags.cern.ch/tc/getReleasesInformation?release_state=Announced
        
        # request related settings (e.g. default injection arguments)
        restapihub.default_sw_version = "CMSSW_5_2_5"
        restapihub.default_sw_scramarch = "slc5_amd64_gcc434"
        restapihub.dqm_url = "https://cmsweb.cern.ch/dqm/dev"
        restapihub.dbs_url = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        
        # web user interface
        ui = views.section_("ui")
        ui.object = "WMCore.ReqMgr.WebGui.FrontPage.FrontPage"
        ui.static_content_dir = path.join(path.abspath(__file__.rsplit('/', 3)[0]),"apps",main.application,"data")
