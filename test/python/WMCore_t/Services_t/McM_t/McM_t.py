#! /bin/env python
"""
Test case for McM service
"""

import unittest

from nose.plugins.attrib import attr

from WMCore.Services.McM.McM import McM

prepID = 'BTV-Upg2023SHCAL14DR-00002'

cert = 'This must be a X509 certificate registered with CERN SSO with access to McM'
key = 'This must be the corresponding key unprotected by a password'


class McMTest(unittest.TestCase):
    """
    Unit tests for McM Service
    """

    @attr("integration")
    def testHistory(self):
        """
        Test that the history URL is working
        """

        history = None
        with McM(cert=cert, key=key) as mcm:
            history = mcm.getHistory(prepID=prepID)

        isAnnounced = False
        for entry in history:
            if entry['action'] == 'set status' and entry['step'] == 'announced':
                isAnnounced = True

        self.assertTrue(isAnnounced)

    @attr("integration")
    def testRequest(self):
        """
        Test that the request URL is working
        """
        request = None
        with McM(cert=cert, key=key) as mcm:
            request = mcm.getRequest(prepID=prepID)

        self.assertTrue('total_events' in request)


if __name__ == '__main__':
    unittest.main()
