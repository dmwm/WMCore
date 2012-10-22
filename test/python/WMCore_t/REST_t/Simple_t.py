from cherrypy.test import test, webtest, helper
from WMCore.REST.Test import setup_test_server, fake_authz_headers
from WMCore.REST.Tools import tools
import WMCore.REST.Test as T
from cherrypy import expose

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

class SimpleTest(helper.CPWebCase):
    def test_basic_fail(self):
        self.getPage("/test")
        self.assertStatus("403 Forbidden")

    def test_basic_success(self):
        h = fake_authz_headers(T.test_authz_key.data)
        self.getPage("/test", headers = h)
        self.assertStatus("200 OK")
        self.assertBody("foo")

    def test_auth_fail(self):
        h = fake_authz_headers(T.test_authz_key.data)
        self.getPage("/test/global_admin", headers = h)
        self.assertStatus("403 Forbidden")

    def test_auth_success(self):
        h = fake_authz_headers(T.test_authz_key.data, roles = {"Global Admin": {'group': ['global']}})
        self.getPage("/test/global_admin", headers = h)
        self.assertStatus("200 OK")
        self.assertBody("ok")

def setup_server():
    srcfile = __file__.split("/")[-1].split(".py")[0]
    setup_test_server(srcfile, "Root")

if __name__ == '__main__':
    setup_server()
    helper.testmain()
