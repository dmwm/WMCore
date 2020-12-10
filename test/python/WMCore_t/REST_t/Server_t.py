from builtins import object
from WMCore.REST.Server import RESTFrontPage
from WMCore.REST.Server import MiniRESTApi
from WMCore.REST.Server import RESTApi
from WMCore.REST.Server import DBConnectionPool
from WMCore.REST.Server import DatabaseRESTApi
from WMCore.REST.Server import RESTEntity
import os, threading

srcfile = os.path.abspath(__file__).rsplit("/", 1)[-1].split(".")[0]
dbspec = {}

class FakeApp(object):
    appname = "app"

class FakeConf(object):
    db = srcfile + ".dbspec"

RESTFrontPage(None, None, "/", "/dev/null", {})
MiniRESTApi(FakeApp(), None, "/")
RESTApi(FakeApp(), None, "/")
DBConnectionPool("x", {})
if threading.current_thread().name == "MainThread":
    DatabaseRESTApi(FakeApp(), FakeConf(), "/")
RESTEntity(FakeApp(), None, None, "/")
