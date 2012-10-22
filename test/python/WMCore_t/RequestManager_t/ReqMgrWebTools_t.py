#!/usr/bin/env python

"""
Unittest for ReqMgr utilities

"""

import unittest
from WMCore.HTTPFrontEnd.RequestManager import ReqMgrWebTools

class ReqMgrWebToolsTest(unittest.TestCase):
    """
    _ReqMgrWebTools_

    Test class for the ReqMgr utility functions

    This ONLY tests things that don't touch the DB
    """

    def setUp(self):
        """
        _setUp_

        Do nothing
        """
        return

    def tearDown(self):
        """
        _tearDown_

        Do nothing
        """
        return

    def testA_ParseRunList(self):
        l0 = ''
        l1 = ' [1,  2,  3 ] '
        l2 = '1,  2, 3   '
        l3 = u'1,  2, 3   '
        l4 = [1,2,3]
        l5 = {1:2, 3:4}
        self.assertEqual(ReqMgrWebTools.parseRunList(l0), [])
        self.assertEqual(ReqMgrWebTools.parseRunList(l1), [1,2,3])
        self.assertEqual(ReqMgrWebTools.parseRunList(l2), [1,2,3])
        self.assertEqual(ReqMgrWebTools.parseRunList(l3), [1,2,3])
        self.assertEqual(ReqMgrWebTools.parseRunList(l4), [1,2,3])

    def testB_ParseBlockList(self):
        l3 = '  ["/test/test/test#Barack", "  /test/test/test#Sarah  ",/test/test/test#George]'
        self.assertEqual(ReqMgrWebTools.parseBlockList(l3), ['/test/test/test#Barack',
                                                             '/test/test/test#Sarah',
                                                             '/test/test/test#George'])
        return

    def testC_RemovePassword(self):
        url = 'http://sarah:maverick@whitehouse.gov:1600/birthcertificates/trig'
        cleanedUrl = ReqMgrWebTools.removePasswordFromUrl(url)
        self.assertEqual(cleanedUrl, 'http://whitehouse.gov:1600/birthcertificates/trig')

    def testD_AllSoftwareVersions(self):
        """
        _AllSoftwareVersions_

        This test is a bit weird because it just checks to make sure you're getting versions
        with CMSSW in the name.  I don't want to do specifics because software versions
        change all the time.
        """

        result = ReqMgrWebTools.allSoftwareVersions()
        self.assertTrue(len(result) > 0)
        for ver in result:
            self.assertTrue('CMSSW' in ver)

        return


if __name__=='__main__':
    unittest.main()
