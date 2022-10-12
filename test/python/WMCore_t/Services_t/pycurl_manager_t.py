"""
Unit test for pycurl_manager module.
"""

from __future__ import division

import gzip
import os
import tempfile
import unittest
import traceback
from Utils.CertTools import getKeyCertFromEnv
from WMCore.Services.pycurl_manager import \
        RequestHandler, ResponseHeader, getdata, cern_sso_cookie, decompress


class PyCurlManager(unittest.TestCase):
    """Test pycurl_manager module"""

    def setUp(self):
        "initialization"
        self.mgr = RequestHandler()
        #self.ckey = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
        #self.cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')
        self.ckey = getKeyCertFromEnv()[0]
        self.cert = getKeyCertFromEnv()[1]

        self.cricheader = 'Date: Tue, 06 Nov 2018 14:50:29 GMT\r\nServer: Apache/2.4.6 (CentOS) OpenSSL/1.0.2k-fips mod_wsgi/3.4 Python/2.7.5 mod_gridsite/2.3.4\r\nVary: Cookie\r\nX-Frame-Options: SAMEORIGIN\r\nSet-Cookie: sessionid=bc1xu8zi5rbbsd5fgjuklb2tk2r3f6tw; expires=Sun, 11-Nov-2018 14:50:29 GMT; httponly; Max-Age=432000; Path=/\r\nContent-Length: 32631\r\nContent-Type: application/json\r\n\r\n'
        self.dbsheader = 'Date: Tue, 06 Nov 2018 14:39:07 GMT\r\nServer: Apache\r\nCMS-Server-Time: D=1503 t=1541515147806112\r\nTransfer-Encoding: chunked\r\nContent-Type: text/html\r\n\r\n'
        self.HTTPheader = 'Date: Tue, 06 Nov 2018 14:50:29 GMT\r\nServer: Apache/2.4.6 (CentOS) OpenSSL/1.0.2k-fips mod_wsgi/3.4 Python/2.7.5 mod_gridsite/2.3.4\r\nVary: Cookie\r\nX-Frame-Options: SAMEORIGIN\r\nSet-Cookie: GRIDHTTP_PASSCODE=2c6da9c96efa2ad0farhda; domain=cms-cric.cern.ch; path=/; secure\r\nContent-Length: 32631\r\nContent-Type: application/json\r\n\r\n'

    def testDecompress(self):
        """
        Test decompress function
        """
        body = "bla"
        headers = {}
        data = decompress(body, headers)
        self.assertEqual(data, body)

        # gzip body
        gzipBody = gzip.compress(bytes(body, 'utf-8'))
        headers = {'Content-Encoding': 'gzip'}
        data = decompress(gzipBody, headers)
        self.assertEqual(data, bytes(body, 'utf-8'))

    def testMulti(self):
        """
        Test fetch of several urls at once, one of the url relies on CERN SSO.
        """
        tfile = tempfile.NamedTemporaryFile()
        url1 = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader/help"
        url2 = "https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader/datatiers"
        url3 = "https://cms-gwmsmon.cern.ch/prodview/json/site_summary"
        cern_sso_cookie(url3, tfile.name, self.cert, self.ckey)
        cookie = {url3: tfile.name}
        urls = [url1, url2, url3]
        data = getdata(urls, self.ckey, self.cert, cookie=cookie)
        headers = 0
        for row in data:
            if '200 OK' in row['headers']:
                headers += 1
        self.assertTrue(headers, 3)

    def testSingle(self):
        """
        Test single call to CERN SSO url.
        """
        # test RequestHandler
        url = "https://cms-gwmsmon.cern.ch/prodview/json/site_summary"
        params = {}
        headers = {"Cache-Control": "no-cache"}
        tfile = tempfile.NamedTemporaryFile()
        cern_sso_cookie(url, tfile.name, self.cert, self.ckey)
        cookie = {url: tfile.name}
        header, _ = self.mgr.request(url, params, headers, cookie=cookie)
        self.assertTrue(header.status, 200)

    def testContinue(self):
        """
        Test HTTP exit code 100 - Continue
        """
        header = "HTTP/1.1 100 Continue\r\n" + self.dbsheader

        resp = ResponseHeader(header)
        self.assertIsNone(getattr(resp, "status", None))
        self.assertEqual(resp.reason, "")
        self.assertFalse(resp.fromcache)
        self.assertIn("CMS-Server-Time", resp.header)
        self.assertIn("Date", resp.header)
        self.assertEqual(resp.header['Content-Type'], 'text/html')
        self.assertEqual(resp.header['Server'], 'Apache')
        self.assertEqual(resp.header['Transfer-Encoding'], 'chunked')
        return

    def testOK(self):
        """
        Test HTTP exit code 200 - OK
        """
        header = "HTTP/1.1 200 OK\r\n" + self.dbsheader

        resp = ResponseHeader(header)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.reason, "OK")
        self.assertFalse(resp.fromcache)
        return

    def testForbidden(self):
        """
        Test HTTP exit code 403 - Forbidden
        """
        header = "HTTP/1.1 403 Forbidden\r\n" + self.dbsheader

        resp = ResponseHeader(header)
        self.assertEqual(resp.status, 403)
        self.assertEqual(resp.reason, "Forbidden")
        self.assertFalse(resp.fromcache)
        return

    def testOKCRIC(self):
        """
        Test HTTP exit code 200 - OK for a CRIC response header
        """
        header = "HTTP/1.1 200 OK\r\n" + self.cricheader

        resp = ResponseHeader(header)
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.reason, "OK")
        self.assertFalse(resp.fromcache)
        self.assertIn("Content-Length", resp.header)
        self.assertIn("Date", resp.header)
        self.assertIn("Server", resp.header)
        self.assertIn("sessionid", resp.header['Set-Cookie'])
        self.assertEqual(resp.header['Content-Type'], 'application/json')
        self.assertEqual(resp.header['Vary'], 'Cookie')
        self.assertEqual(resp.header['X-Frame-Options'], 'SAMEORIGIN')
        return

    def testUnavailableCRICHTTP(self):
        """
        Test HTTP exit code 503 - Service Unavailable for a CRIC response header
        when it also contains a HTTP string in the Set-Cookie header section
        """
        header = "HTTP/1.1 503 Service Unavailable\r\n" + self.HTTPheader

        resp = ResponseHeader(header)
        self.assertEqual(resp.status, 503)
        self.assertEqual(resp.reason, "Service Unavailable")
        self.assertFalse(resp.fromcache)
        self.assertIn("Content-Length", resp.header)
        self.assertIn("Date", resp.header)
        self.assertIn("Server", resp.header)
        self.assertIn("GRIDHTTP_PASSCODE", resp.header['Set-Cookie'])
        self.assertEqual(resp.header['Content-Type'], 'application/json')
        self.assertEqual(resp.header['Vary'], 'Cookie')
        self.assertEqual(resp.header['X-Frame-Options'], 'SAMEORIGIN')
        return

    def testHeadRequest(self):
        """
        Test a HEAD request.
        """
        params = {}
        headers = {}
        url = 'https://cmsweb-testbed.cern.ch/reqmgr2/data/info'
        res = self.mgr.getheader(url, params=params, headers=headers, ckey=self.ckey, cert=self.cert)
        self.assertEqual(res.getReason(), "OK")
        self.assertTrue(len(res.getHeader()) > 10)
        # Kubernetes cluster responds with a different Server header
        serverHeader = res.getHeaderKey("Server")
        self.assertTrue(serverHeader.startswith("nginx/") or serverHeader.startswith("CherryPy/") or serverHeader.startswith("openresty/"))

    def testHeadGzip(self):
        """
        Test a HEAD request incorrectly asking for gzip body
        """
        params = {}
        headers = {'Accept-Encoding': 'gzip'}
        url = 'https://cmsweb-testbed.cern.ch/reqmgr2/data/info'
        res = self.mgr.getdata(url, params=params, headers=headers, verb="HEAD",
                               ckey=self.ckey, cert=self.cert)
        self.assertEqual(res, "", "There is no body in HEAD requests")

        res = self.mgr.getheader(url, params=params, headers=headers, verb="HEAD",
                                 ckey=self.ckey, cert=self.cert)
        self.assertEqual(res.getReason(), "OK")
        self.assertEqual(res.getHeaderKey("Content-Encoding"), "gzip")
        # Kubernetes cluster responds with a different Server header
        serverHeader = res.getHeaderKey("Server")
        self.assertTrue(serverHeader.startswith("nginx/") or serverHeader.startswith("CherryPy/") or serverHeader.startswith("openresty/"))

    def testHeadUnsupportedAPI(self):
        """
        Test HEAD http request to an wrong endpoint (unsupported API),
        either with and without encoding compression (gzip)
        """
        params = {}
        url = 'https://cmsweb-testbed.cern.ch/reqmgr2/data/wrong_endpoint'
        for headers in [{}, {'Accept-Encoding': 'gzip'}]:
            try:
                res = self.mgr.getdata(url, params=params, headers=headers, verb="HEAD",
                                       ckey=self.ckey, cert=self.cert)
            except Exception as exc:
                self.assertTrue("404 Not Found" in str(exc))
            else:
                self.assertTrue("404 Not Found" in res)

            try:
                res = self.mgr.getheader(url, params=params, headers=headers, verb="HEAD",
                                         ckey=self.ckey, cert=self.cert)
            except Exception as exc:
                self.assertTrue("404 Not Found" in str(exc))
            else:
                self.assertEqual(res.getReason(), "Not Found")
                self.assertEqual(res.getHeaderKey("X-Error-Detail"), "API not supported")
                self.assertIsNone(res.getHeaderKey("Content-Encoding"))

    def testGetUnsupportedAPI(self):
        """
        Test GET http request to an wrong endpoint (unsupported API),
        either with and without encoding compression (gzip)
        """
        params = {}
        url = 'https://cmsweb-testbed.cern.ch/reqmgr2/data/wrong_endpoint'
        for headers in [{}, {'Accept-Encoding': 'gzip'}]:
            try:
                res = self.mgr.getdata(url, params=params, headers=headers, ckey=self.ckey, cert=self.cert)
            except Exception as exc:
                self.assertTrue("404 Not Found" in str(exc))
            else:
                self.assertTrue("404 Not Found" in res)

            try:
                res = self.mgr.getheader(url, params=params, headers=headers, ckey=self.ckey, cert=self.cert)
            except Exception as exc:
                self.assertTrue("404 Not Found" in str(exc))
            else:
                self.assertEqual(res.getReason(), "Not Found")
                self.assertEqual(res.getHeaderKey("X-Error-Detail"), "API not supported")
                self.assertIsNone(res.getHeaderKey("Content-Encoding"))

    def testToken(self):
        """
        Test setting up token header
        """
        iam_token = "eyJraWQiOiJyc2ExIiwiYWxnIjoiUlMyNTYifQ.eyJ3bGNnLnZlciI6IjEuMCIsInN1YiI6IjZjYTE3YzMyLTU0YWUtNDUzYy04YjQ1LTYyNTFkMjhlOTRhMSIsImF1ZCI6Imh0dHBzOlwvXC93bGNnLmNlcm4uY2hcL2p3dFwvdjFcL2FueSIsIm5iZiI6MTY0NTU1MDExMCwic2NvcGUiOiJhZGRyZXNzIHBob25lIG9wZW5pZCBvZmZsaW5lX2FjY2VzcyBwcm9maWxlIGVkdXBlcnNvbl9zY29wZWRfYWZmaWxpYXRpb24gZWR1cGVyc29uX2VudGl0bGVtZW50IGVtYWlsIHdsY2ciLCJpc3MiOiJodHRwczpcL1wvY21zLWF1dGgud2ViLmNlcm4uY2hcLyIsImV4cCI6MTY0NTU1MzcxMCwiaWF0IjoxNjQ1NTUwMTEwLCJqdGkiOiJkMWE3NzI0MC02Mjk5LTRiM2MtYmY5ZS0yOGNmYzgxY2ZjZDciLCJjbGllbnRfaWQiOiI0N2EwYjZkMC1mMzBlLTQ2OGItYmMwYy01MWVlNmE5Nzg2ODAifQ.Rpw6Mk_QzxtQVcbS-2OeVYGNMzyO08W540Sv3Nda2x3UJmPBRK4lnriRgSzMNTGP4y51rC5exbpf970MpJrjPaWqDhFWC--T6hxPAHhxHUxMTkXF5hUGXMLoNkCN5yR4eBJSAbgrgYJJwBcY5lMdCQ0BH5kQAUL6XRr3kvCrQQ0"
        os.environ['IAM_TOKEN'] = iam_token
        url = 'https://cmsweb-testbed.cern.ch'
        params = {}
        headers = {}
        try:
            # this call requires that our url (cmsweb-testbed) will
            # accept token based header, therefore we wrap it up in
            # try/except block to prevent from failing until
            # token based auth will be in place
            res = self.mgr.getheader(url, params=params, headers=headers, ckey=self.ckey, cert=self.cert)
            if isinstance(self.mgr.request_headers, dict):
                auth_header = self.mgr.request_headers.get('Authorization')
                self.assertTrue(auth_header != "", True)
            # the following test should be true only if we have valid token
            # TODO: so far I don't know how to test this since it requires
            # to obtain valid token
            # self.assertTrue(auth_header == 'Bearer {}'.format(iam_token), True)
        except Exception as exc:
            # do not use print since it cause E1601 ("print statement used"), see
            # https://github.com/PyCQA/pylint/issues/437
            traceback.print_exc()

if __name__ == "__main__":
    unittest.main()
