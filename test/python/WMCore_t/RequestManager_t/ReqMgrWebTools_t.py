"""
Unittest for ReqMgr utilities

"""

import unittest
from WMCore.HTTPFrontEnd.RequestManager import ReqMgrWebTools 
from WMCore.RequestManager.RequestMaker.CheckIn import RequestCheckInError


class ReqMgrWebToolsTest(unittest.TestCase):
    """
    Test class for the ReqMgr utility functions

    This ONLY tests things that don't touch the DB
    
    """

    def setUp(self):
        """
        _setUp_

        Do nothing
        
        """
        pass


    def tearDown(self):
        """
        _tearDown_

        Do nothing
        
        """
        pass

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

    def testCheckinRaiseError(self):
        
        try:
            try:
                raise RequestCheckInError("Error in Request check-in: blah")
            except RequestCheckInError, ex:
                raise ex
        except RequestCheckInError, ex:
            print str(ex)
            
if __name__=='__main__':
    unittest.main()