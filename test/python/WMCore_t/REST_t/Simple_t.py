# system modules
import cherrypy
from cheroot.test import webtest
from cherrypy import expose
from multiprocessing import Process

# WMCore modules
from WMCore.REST.Auth import user_info_from_headers
from WMCore.REST.Test import setup_dummy_server, fake_authz_headers
from WMCore.REST.Test import fake_authz_key_file
from WMCore.REST.Tools import tools

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
        self.engine = cherrypy.engine
        self.proc = load_server(self.engine)

    def tearDown(self):
        stop_server(self.proc, self.engine)

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

class AuthTest(webtest.WebCase, cherrypy.Tool):

    def setUp(self):
        cherrypy.Tool.__init__(self, 'before_request_body', user_info_from_headers, priority=60)
        webtest.WebCase.PORT = PORT
        self.engine = cherrypy.engine
        self.proc = load_server(self.engine)

    def tearDown(self):
        print("teardown")
        stop_server(self.proc, self.engine)

    def testAuth(self):
        myHeaders = [('cms-authn-name', 'Blah'), ('cms-auth-status', 'OK'),
                     ('cms-authn-login', 'blah'), ('cms-authn-hmac', '1234')]
        self.getPage("/test", headers=myHeaders)
        self.assertTrue(True)  # Do not remove this line! otherwise the test hangs


def setup_server():
    srcfile = __file__.split("/")[-1].split(".py")[0]
    setup_dummy_server(srcfile, "Root", authz_key_file=FAKE_FILE, port=PORT)

def load_server(engine):
    setup_server()
    proc = Process(target=start_server, name="cherrypy_Api_t", args=(engine,))
    proc.start()
    proc.join(timeout=1)
    return proc

def start_server(engine):
    webtest.WebCase.PORT = PORT
    cherrypy.log.screen = True
    engine.start()
    engine.block()

def stop_server(proc, engine):
    cherrypy.log.screen = True
    engine.stop()
    proc.terminate()

if __name__ == '__main__':
    webtest.main()
