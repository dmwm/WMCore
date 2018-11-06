"""
Unit test for pycurl_manager module.
"""

from __future__ import division

import os
import tempfile
import unittest

from WMCore.Services.pycurl_manager import RequestHandler, ResponseHeader, getdata, cern_sso_cookie


class PyCurlManager(unittest.TestCase):
    """Test pycurl_manager module"""

    def setUp(self):
        "initialization"
        self.mgr = RequestHandler()
        self.ckey = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
        self.cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')

        self.cricheader = 'Date: Tue, 06 Nov 2018 14:50:29 GMT\r\nServer: Apache/2.4.6 (CentOS) OpenSSL/1.0.2k-fips mod_wsgi/3.4 Python/2.7.5 mod_gridsite/2.3.4\r\nVary: Cookie\r\nX-Frame-Options: SAMEORIGIN\r\nSet-Cookie: sessionid=bc1xu8zi5rbbsd5fgjuklb2tk2r3f6tw; expires=Sun, 11-Nov-2018 14:50:29 GMT; httponly; Max-Age=432000; Path=/\r\nContent-Length: 32631\r\nContent-Type: application/json\r\n\r\n'
        self.dbsheader = 'Date: Tue, 06 Nov 2018 14:39:07 GMT\r\nServer: Apache\r\nCMS-Server-Time: D=1503 t=1541515147806112\r\nTransfer-Encoding: chunked\r\nContent-Type: text/html\r\n\r\n'
        self.HTTPheader = 'Date: Tue, 06 Nov 2018 14:50:29 GMT\r\nServer: Apache/2.4.6 (CentOS) OpenSSL/1.0.2k-fips mod_wsgi/3.4 Python/2.7.5 mod_gridsite/2.3.4\r\nVary: Cookie\r\nX-Frame-Options: SAMEORIGIN\r\nSet-Cookie: GRIDHTTP_PASSCODE=2c6da9c96efa2ad0farhda; domain=cms-cric.cern.ch; path=/; secure\r\nContent-Length: 32631\r\nContent-Type: application/json\r\n\r\n'

    def testMulti(self):
        """
        Test fetch of several urls at once, one of the url relies on CERN SSO.
        """
        tfile = tempfile.NamedTemporaryFile()
        url1 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/help"
        url2 = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datatiers"
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
        tfile = tempfile.NamedTemporaryFile()
        cern_sso_cookie(url, tfile.name, self.cert, self.ckey)
        cookie = {url: tfile.name}
        header, _ = self.mgr.request(url, params, cookie=cookie)
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


if __name__ == "__main__":
    unittest.main()
