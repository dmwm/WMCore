'''
Created on Oct 4, 2011

@author: meloam
'''
import unittest

from WMCore.Services.EmulatorSwitch import EmulatorHelper


class EmulatorSwitch_t(unittest.TestCase):


    def testGetEmulators(self):
        from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
        phedexJSON = PhEDEx(responseType='json')
        self.assertTrue( hasattr( phedexJSON, '_testNonExistentInEmulator' ) )
        EmulatorHelper.setEmulators(phedex = True, dbs = True,
                                    siteDB = True, requestMgr = False)
        phedexJSON2 = PhEDEx(responseType='json')
        self.assertFalse( hasattr( phedexJSON2, '_testNonExistentInEmulator' ) )
        EmulatorHelper.resetEmulators()
        phedexJSON2 = PhEDEx(responseType='json')
        self.assertTrue( hasattr( phedexJSON2, '_testNonExistentInEmulator' ) )



    def tearDown(self):
        EmulatorHelper.resetEmulators()
        pass

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
