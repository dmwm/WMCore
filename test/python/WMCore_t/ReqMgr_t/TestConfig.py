"""
ReqMgr unittest configuration file.

"""

# ---------------------------------------------------------------------------
# this entire section copied from:
#     deployment/reqmgr2/config.py


import socket
from os import path, getenv

from WMCore.Configuration import Configuration

HOST = socket.gethostname().lower()
BASE_URL = "@@BASE_URL@@"
DBS_INS = "@@DBS_INS@@"
COUCH_URL = "%s/couchdb" % BASE_URL
LOG_DB_URL = "%s/wmstats_logdb" % COUCH_URL
LOG_REPORTER = "reqmgr2"

ROOTDIR = __file__.rsplit('/', 3)[0]

config = Configuration()

main = config.section_("main")
srv = main.section_("server")
srv.thread_pool = 30
srv.accepted_queue_size = -1
srv.accepted_queue_timeout = 0
main.application = "reqmgr2"
main.port = 9988  # main application port it listens on
main.index = "ui"
# Defaults to allow any CMS authenticated user. Write APIs should require
# additional roles in CRIC (i.e. "Admin" role for the "ReqMgr" group)
main.authz_defaults = {"role": None, "group": None, "site": None}
main.log_screen = True

tools = main.section_("tools")
# provide CherryPy monitoring under: <hostname>/reqmgr2/data/stats
tools.section_("cpstats").on = False
tools.section_("cms_auth").key_file = "%s/auth/wmcore-auth/header-auth-key" % ROOTDIR

# this is where the application will be mounted, where the REST API
# is reachable and this features in CMS web frontend rewrite rules
app = config.section_(main.application)  # string containing "reqmgr2"
app.admin = "cms-service-webtools@cern.ch"
app.description = "CMS data operations Request Manager."
app.title = "CMS Request Manager (ReqMgr)"

views = config.section_("views")

# practical to have this kind of configuration values not in
# service related RPM (difficult/impossible to change in CMS web
# deployment) but in the deployment configuration for the service

# redirector for the REST API implemented handlers
data = views.section_("data")
data.object = "WMCore.ReqMgr.Service.RestApiHub.RestApiHub"
# The couch host is defined during deployment time.
data.couch_host = COUCH_URL
# main ReqMgr CouchDB database containing all requests with spec files attached
data.couch_reqmgr_db = "reqmgr_workload_cache"
# ReqMgr database containing groups, teams, software, etc
data.couch_reqmgr_aux_db = "reqmgr_auxiliary"
# ConfigCache - database with configuration documents
data.couch_config_cache_db = "reqmgr_config_cache"
data.couch_workload_summary_db = "workloadsummary"
data.couch_wmstats_db = "wmstats"
data.couch_acdc_db = "acdcserver"
data.couch_workqueue_db = "workqueue"
data.central_logdb_url = LOG_DB_URL
data.log_reporter = LOG_REPORTER
# Fake permissions for unit tests
data.authorized_roles = {"admin": {'role': ["admin_role"], 'group': ["admin_group"]},
                         "ops": {'role': ["admin_role", "ops_role"],
                                 'group': ["admin_group", "ops_group"]},
                         "ppd": {'role': ["admin_role", "ops_role", "ppd_role"],
                                 'group': ["admin_group", "ops_group", "ppd_group"]}}
data.authz_by_status = [{"permission": "admin",
                         "statuses": ["acquired", "running-open", "running-closed", "completed", "aborted-completed",
                                      "failed", "rejected-archived", "aborted-archived", "normal-archived"]},
                        {"permission": "ops",
                         "statuses": ["assigned", "staging", "staged", "force-complete",
                                      "closed-out", "announced"]},
                        {"permission": "ppd",
                         "statuses": ["new", "assignment-approved", "rejected", "aborted", "NO_STATUS"]}]

# number of past days since when to display requests in the default view
data.default_view_requests_since_num_days = 30  # days
# resource to fetch CMS software versions and scramarch info from
data.tag_collector_url = "https://cmssdt.cern.ch/SDT/cgi-bin/ReleasesXML"
# another source at TC, returns directly JSON, but strangely formatted (e.g.
# keys are not present at easy item but defined in a dedicated item ...)
# https://cmssdt.cern.ch/tc/getReleasesInformation?release_state=Announced

# use dbs testbed for private vm test
data.dbs_url = "https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader"

# web user interface
ui = views.section_("ui")
ui.object = "WMCore.ReqMgr.WebGui.FrontPage.FrontPage"
ui.static_content_dir = path.join(path.abspath(ROOTDIR),
                                  "apps",
                                  main.application,
                                  "data")

# end of deployment/reqmgr2/config.py
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# this entire section copied from:
#     deployment/reqmgr2/config-localhost.py


# localhost running additions, no authentication
config.main.section_("tools")
config.main.tools.section_("cms_auth")
config.main.server.socket_host = "127.0.0.1"
config.main.server.environment = "staging"  # must not be "production"
# config.main.tools.cms_auth.policy = "dangerously_insecure"

# go up /deployment/reqmgr2/__file__
first_part = path.abspath(ROOTDIR)
config.views.ui.static_content_dir = path.join(first_part, "WMCore/src/data")

config.views.data.couch_host = getenv("COUCHURL", None)

# end of deployment/reqmgr2/config-localhost.py
# ---------------------------------------------------------------------------
