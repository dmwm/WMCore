# system modules
import cherrypy
from cherrypy.test import webtest
from cherrypy import expose
from multiprocessing import Process

# WMCore modules
from WMCore.REST.Test import setup_test_server, fake_authz_headers
from WMCore.REST.Test import fake_authz_key_file
from WMCore.REST.Tools import tools
import WMCore.REST.Test as T

FAKE_FILE = fake_authz_key_file()
PORT = 8888

class Root:
    def __init__(self, *args):
        pass

    @expose
    def default(self):
        return "foo"

    @expose
    @tools.cms_auth(role = "Global Admin", group = "global")
    def global_admin(self):
        return "ok"

class SimpleTest(webtest.WebCase):

    def setUp(self):
        self.h = fake_authz_headers(FAKE_FILE.data)
        self.hglobal = fake_authz_headers(FAKE_FILE.data, roles = {"Global Admin": {'group': ['global']}})
        webtest.WebCase.PORT = PORT
        self.proc = load_server()

    def tearDown(self):
        self.proc.terminate()
        cherrypy.engine.exit()

    def test_basic_fail(self):
        self.getPage("/test")
        self.assertStatus("403 Forbidden")

    def test_basic_success(self):
        self.getPage("/test", headers = self.h)
        self.assertStatus("200 OK")
        self.assertBody("foo")

    def test_auth_fail(self):
        self.getPage("/test/global_admin", headers = self.h)
        self.assertStatus("403 Forbidden")

    def test_auth_success(self):
        self.getPage("/test/global_admin", headers = self.hglobal)
        self.assertStatus("200 OK")
        self.assertBody("ok")

def setup_server():
    srcfile = __file__.split("/")[-1].split(".py")[0]
    setup_test_server(srcfile, "Root", authz_key_file=FAKE_FILE, port=PORT)

def load_server():
    setup_server()
    proc = Process(target=start_server, name="cherrypy_test_server")
    proc.start()
    proc.join(timeout=1)
    return proc

def start_server():
    webtest.WebCase.PORT = PORT
    cherrypy.engine.start()
    cherrypy.engine.block()

if __name__ == '__main__':
    webtest.main()
