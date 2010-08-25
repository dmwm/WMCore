"""
"""
import unittest
import os
import logging
from WMCore.Services.Service import Service

class ServiceTest(unittest.TestCase):
    def setUp(self):
        """
        Setup for unit tests
        """
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='jsonparser.log',
                    filemode='w')

        dict = {'logger': logging.getLogger('JSONParser'), 
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi'}
        
        self.myService = Service(dict)
        self.testUrl = 'http://cern.ch'
    
    def runTest(self):
        self.testClear()
     
    def testClear(self):
        """
        Populate the cache, and then check that it's deleted
        """
        f = self.myService.refreshCache('testClear', '/COMP/WMCORE/src/python/WMCore/Services/Service.py?view=markup')
        assert os.path.exists(f.name)
        f.close()
        
        self.myService.clearCache('testClear', '/COMP/WMCORE/src/python/WMCore/Services/Service.py?view=markup')
        assert not os.path.exists(f.name)
        

if __name__ == '__main__':
    unittest.main()
