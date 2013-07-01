
"""
WMStats only configuration file.

"""


from WMCore.Configuration import Configuration
from os import path

class Config(Configuration):

    def __init__(self, appport, couchurl, secure=True):
        Configuration.__init__(self)
        main = self.section_("main")
        srv = main.section_("server")
        srv.thread_pool = 30
        main.application = "wmstats"
        main.port = appport # main application port it listens on
        main.index = "wmstats"
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
        app.title = "CMS Request Monitor (ReqMon)"
        
        views = self.section_("views")
        
        # redirector for the REST API implemented handlers
        wmstats = views.section_("wmstats")
        wmstats.object = "WMCore.WMStats.WMStatsRESTApi.WMStatsRESTApi"
        # The couch host is defined during deployment time.
        wmstats.reqmgrCouchURL = "%s/%s" % (couchurl, "reqmgr_workload_cache")
        wmstats.wmstatsCouchURL = "%s/%s" % (couchurl, "wmstats")
        
        # web user interface
        ui = views.section_("ui")
        ui.object = "WMCore.WMStats.FrontPage.FrontPage"
        ui.static_content_dir = path.join(path.abspath(__file__.rsplit('/', 3)[0]),"apps",main.application,"data")
