"""
MicroService configuration file.
"""
from __future__ import division

import os
import socket
from WMCore.Configuration import Configuration

# globals
HOST = socket.gethostname().lower()
ROOTDIR = os.getenv('MS_STATIC_ROOT', os.getcwd())
config = Configuration()

main = config.section_("main")
srv = main.section_("server")
srv.thread_pool = 30
main.application = "microservice"
main.port = 8833  # main application port it listens on
main.index = 'ui' # Configuration requires index attribute
# Security configuration
#main.authz_defaults = {"role": None, "group": None, "site": None}
#sec = main.section_("tools").section_("cms_auth")
#sec.key_file = "%s/auth/wmcore-auth/header-auth-key" % ROOTDIR

# this is where the application will be mounted, where the REST API
# is reachable and this features in CMS web frontend rewrite rules
app = config.section_(main.application)
app.admin = "cms-service-webtools@cern.ch"
app.description = "CMS data operations MicroService"
app.title = "CMS MicroService"

# define different views for our application
views = config.section_("views")
# web UI interface
ui = views.section_('ui')
ui.object = 'WMCore.MicroService.WebGui.FrontPage.FrontPage'
ui.static = ROOTDIR

# REST interface
data = views.section_('data')
data.object = 'WMCore.MicroService.Service.RestApiHub.RestApiHub'
data.manager = 'WMCore.MicroService.MSManager.MSManager'
