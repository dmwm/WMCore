"""
Unit test for pycurl_manager module.
"""

from __future__ import division

import os
import tempfile
import unittest

from WMCore.Services.pycurl_manager import RequestHandler, getdata, cern_sso_cookie

class PyCurlManager(unittest.TestCase):
    """Test pycurl_manager module"""

    def setUp(self):
        "initialization"
        self.mgr = RequestHandler()
        self.ckey = os.path.join(os.environ['HOME'], '.globus/userkey.pem')
        self.cert = os.path.join(os.environ['HOME'], '.globus/usercert.pem')

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

if __name__ == "__main__":
    unittest.main()
