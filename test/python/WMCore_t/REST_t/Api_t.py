# python-future
from builtins import range

# system modules
import json
import re
import zlib
from multiprocessing import Process

import cherrypy
from cherrypy import response
from cheroot.test import webtest

# WMCore modules
from WMCore.REST.Server import RESTApi, RESTEntity, restcall, rows
from WMCore.REST.Test import setup_dummy_server, fake_authz_headers
from WMCore.REST.Test import fake_authz_key_file
from WMCore.REST.Validation import validate_num, validate_str
from WMCore.REST.Error import InvalidObject
from WMCore.REST.Format import RawFormat
from WMCore.REST.Tools import tools

gif_bytes = (b'GIF89a\x01\x00\x01\x00\x82\x00\x01\x99"\x1e\x00\x00\x00\x00\x00'
             b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
             b'\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x02\x03\x02\x08\t\x00;')

FAKE_FILE = fake_authz_key_file()
PORT = 8887

class Simple(RESTEntity):
    def validate(self, *args): pass

    @restcall
    @tools.expires(secs=300)
    def get(self):
        return rows(['foo'])

class Multi(RESTEntity):
    def validate(self, apiobj, method, api, param, safe):
        validate_str("etag", param, safe, re.compile("^[a-z]*$"), optional=True)
        validate_num("lim", param, safe, bare=True, optional=True, minval=0, maxval=10)

    def _generate(self, lim):
        for i in range(0, 10):
            if i == lim:
                raise InvalidObject("cut at %d" % i)
            yield ["row", i]

        if i == lim:
            raise InvalidObject("cut at %d" % i)

    @restcall
    @tools.expires(secs=300)
    def get(self, lim, etag):
        if etag:
            response.headers["ETag"] = '"%s"' % etag

        if lim == 0:
            raise InvalidObject("cut at 0")

        return self._generate(lim)

class Image(RESTEntity):
    def validate(self, *args): pass

    @restcall(formats=[("image/gif", RawFormat())])
    @tools.expires(secs=300)
    def get(self):
        return gif_bytes

class Root(RESTApi):
    def __init__(self, app, config, mount):
        RESTApi.__init__(self, app, config, mount)
        self._add({ "simple": Simple(app, self, config, mount),
                    "image":  Image(app, self, config, mount),
                    "multi":  Multi(app, self, config, mount) })

class Tester(webtest.WebCase):

    def setUp(self):
        self.h = fake_authz_headers(FAKE_FILE.data)
        webtest.WebCase.PORT = PORT
        self.engine = cherrypy.engine
        self.proc = load_server(self.engine)

    def tearDown(self):
        stop_server(self.proc, self.engine)

    def _test_accept_ok(self, fmt, page = "/test/simple", inbody = None):
        h = self.h + [("Accept", fmt)]
        self.getPage(page, headers = h)
        self.assertStatus("200 OK")
        if fmt.find("*") >= 0:
            self.assertHeader("Content-Type")
        else:
            self.assertHeader("Content-Type", fmt)
        self.assertHeader("X-REST-Status", "100")
        self.assertHeader("X-REST-Time")
        self.assertNoHeader("X-Error-ID")
        self.assertNoHeader("X-Error-HTTP")
        self.assertNoHeader("X-Error-Info")
        self.assertNoHeader("X-Error-Detail")
        if inbody:
            self.assertInBody(inbody)

    def _test_accept_fail(self, fmt, page="/test/simple",
                          avail="application/json, application/xml"):
        h = self.h + [("Accept", fmt)]
        self.getPage(page, headers = h)
        self.assertStatus("406 Not Acceptable")
        self.assertHeader("X-REST-Status", "201")
        self.assertHeader("X-REST-Time")
        self.assertHeader("X-Error-ID")
        self.assertHeader("X-Error-HTTP", "406")
        self.assertHeader("X-Error-Info", "Available types: %s" % avail)
        self.assertHeader("X-Error-Detail", "Not acceptable")
        self.assertInBody("Not acceptable")

    def test_accept_good_json(self):
        self._test_accept_ok("application/json", inbody="foo")

    def test_accept_good_xml(self):
        self._test_accept_ok("application/xml", inbody="foo")

    def test_accept_good_gif(self):
        self._test_accept_ok("image/gif", page="/test/image")
        self.assertBody(gif_bytes)

    def test_accept_good_gif2(self):
        self._test_accept_ok("image/*", page="/test/image")
        self.assertBody(gif_bytes)

    def test_accept_reject_octets(self):
        self._test_accept_fail("application/octet-stream")

    def test_accept_reject_xfoo(self):
        self._test_accept_fail("application/x-foo")

    def test_accept_reject_gif(self):
        self._test_accept_fail("image/gif")

    def test_accept_reject_gif2(self):
        self._test_accept_fail("image/png", page="/test/image", avail="image/gif")

    def test_accept_reject_png(self):
        self._test_accept_fail("image/png")

    def test_simple_json(self):
        h = self.h
        h.append(("Accept", "application/json"))
        self.getPage("/test/simple", headers = h)
        self.assertStatus("200 OK")
        b = json.loads(self.body)
        assert isinstance(b, dict)
        assert "desc" not in b
        assert "result" in b
        assert isinstance(b["result"], list)
        assert len(b["result"]) == 1
        assert b["result"][0] == "foo"

    def test_simple_json_deflate(self):
        h = self.h
        h.append(("Accept", "application/json"))
        h.append(("Accept-Encoding", "deflate"))
        self.getPage("/test/simple", headers = h)
        self.assertStatus("200 OK")
        self.assertHeader("Content-Length")
        self.assertHeader("Content-Encoding", "deflate")
        b = json.loads(zlib.decompress(self.body, -zlib.MAX_WBITS))
        assert isinstance(b, dict)
        assert "desc" not in b
        assert "result" in b
        assert isinstance(b["result"], list)
        assert len(b["result"]) == 1
        assert b["result"][0] == "foo"

    def test_multi_nothrow(self):
        h = self.h
        h.append(("Accept", "application/json"))
        self.getPage("/test/multi", headers = h)
        self.assertStatus("200 OK")
        self.assertHeader("X-REST-Status", "100")
        b = json.loads(self.body)
        assert isinstance(b, dict)
        assert "desc" not in b
        assert "result" in b
        assert isinstance(b["result"], list)
        assert len(b["result"]) == 10
        for i in range(0, 10):
            assert isinstance(b["result"][i], list)
            assert b["result"][i][0] == "row"
            assert b["result"][i][1] == i

    def test_multi_throw0(self):
        h = self.h
        h.append(("Accept", "application/json"))
        self.getPage("/test/multi?lim=0", headers = h)
        self.assertStatus(400)
        self.assertHeader("X-REST-Status", "306")
        self.assertHeader("X-Error-HTTP", "400")
        self.assertHeader("X-Error-Info", "cut at 0")
        self.assertHeader("X-Error-Detail", "Invalid object")
        self.assertHeader("X-Error-ID")

    def test_multi_throw5a(self):
        h = self.h
        h.append(("Accept", "application/json"))
        self.getPage("/test/multi?lim=5&etag=x", headers = h)
        self.assertStatus("200 OK")
        self.assertHeader("X-REST-Status", "100")
        b = json.loads(self.body)
        assert isinstance(b, dict)
        assert "desc" not in b
        assert "result" in b
        assert isinstance(b["result"], list)
        assert len(b["result"]) == 5
        for i in range(0, 5):
            assert isinstance(b["result"][i], list)
            assert b["result"][i][0] == "row"
            assert b["result"][i][1] == i

    def test_multi_throw5b(self):
        h = self.h
        h.append(("Accept", "application/json"))
        self.getPage("/test/multi?lim=5", headers = h)
        self.assertStatus(400)
        self.assertHeader("X-REST-Status", "200")
        self.assertHeader("X-Error-HTTP", "400")
        self.assertHeader("X-Error-Info", "cut at 5")
        self.assertHeader("X-Error-Detail", "Invalid object")
        self.assertHeader("X-Error-ID")

    def test_multi_throw10(self):
        h = self.h
        h.append(("Accept", "application/json"))
        self.getPage("/test/multi?lim=10&etag=x", headers = h)
        self.assertStatus("200 OK")
        self.assertHeader("X-REST-Status", "100")
        b = json.loads(self.body)
        assert isinstance(b, dict)
        assert "desc" not in b
        assert "result" in b
        assert isinstance(b["result"], list)
        assert len(b["result"]) == 10
        for i in range(0, 10):
            assert isinstance(b["result"][i], list)
            assert b["result"][i][0] == "row"
            assert b["result"][i][1] == i

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
