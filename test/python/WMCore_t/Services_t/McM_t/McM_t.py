#! /bin/env python
"""
Test case for McM service
"""

import unittest
from nose.plugins.attrib import attr
from WMCore.Services.McM.McM import McM

prepID = 'BTV-Upg2023SHCAL14DR-00002'


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
        with McM() as mcm:
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
        with McM() as mcm:
            request = mcm.getRequest(prepID=prepID)

        self.assertTrue('total_events' in request)

    @attr("integration")
    def testHistoryDevelopmentEnv(self):
        """
        Test that the history URL is working.
        McM Development has been migrated to the new SSO
        (Keycloak)
        """

        history = None
        development_url: str = "https://cms-pdmv-dev.cern.ch/mcm"
        with McM(url=development_url) as mcm:
            history = mcm.getHistory(prepID=prepID)

        isAnnounced = False
        for entry in history:
            if entry['action'] == 'set status' and entry['step'] == 'announced':
                isAnnounced = True

        self.assertTrue(isAnnounced)

    @attr("integration")
    def testRequestDevelopmentEnv(self):
        """
        Test that the request URL is working.
        McM Development has been migrated to the new SSO
        (Keycloak)
        """
        request = None
        development_url: str = "https://cms-pdmv-dev.cern.ch/mcm"
        with McM(url=development_url) as mcm:
            request = mcm.getRequest(prepID=prepID)

        self.assertTrue('total_events' in request)


if __name__ == '__main__':
    unittest.main()
