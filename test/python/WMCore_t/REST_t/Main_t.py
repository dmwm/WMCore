from WMCore.REST.Main import ProfiledApp
from WMCore.REST.Main import Logger
from WMCore.REST.Main import RESTMain
from WMCore.REST.Main import RESTDaemon
from WMCore.REST.Test import fake_authz_key_file
from WMCore.Configuration import Configuration
from cherrypy import Application
import os

def dummy():
    return ""

import os

authz_key = fake_authz_key_file()
cfg = Configuration()
main = cfg.section_('main')
main.application = 'test'
main.silent = True
main.index = 'top'
main.authz_defaults = { 'role': None, 'group': None, 'site': None }
main.section_('tools').section_('cms_auth').key_file = authz_key.name
app = cfg.section_('test')
app.admin = 'dada@example.org'
app.description = app.title = 'Test'
views = cfg.section_('views')
top = views.section_('top')
top.object = os.path.abspath(__file__).rsplit("/", 1)[-1].split(".")[0] + ".Test"

ProfiledApp(Application(dummy, "/", {"/": {}}), ".")
Logger()
RESTMain(cfg, ".")
RESTDaemon(cfg, ".")
