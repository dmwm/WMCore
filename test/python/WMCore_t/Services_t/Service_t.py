"""
"""
import unittest
import os
import logging
import socket
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

        
    def testClear(self):
        """
        Populate the cache, and then check that it's deleted
        """
        f = self.myService.refreshCache('testClear', '/COMP/WMCORE/src/python/WMCore/Services/Service.py?view=markup')
        assert os.path.exists(f.name)
        f.close()
        
        self.myService.clearCache('testClear')
        assert not os.path.exists(f.name)
        
    def testCachePath(self):
        dict = {'logger': logging.getLogger('JSONParser'), 
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cachepath': '/my/path'}
        service = Service(dict)
        assert service['cachepath'] == dict['cachepath']
    
    def testCacheDuration(self):
        dict = {'logger': logging.getLogger('JSONParser'), 
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': 100}
        service = Service(dict)
        assert service['cacheduration'] == dict['cacheduration']
        
    def testNoCacheDuration(self):
        dict = {'logger': logging.getLogger('ServiceTest'), 
                'endpoint':'http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi',
                'cacheduration': None}
        service = Service(dict)
        assert service['cacheduration'] == dict['cacheduration']
        
    def testSocketTimeout(self):
        dict = {'logger': logging.getLogger('ServiceTest'), 
                'endpoint':'http://cmssw.cvs.cern.ch',
                'cacheduration': None,
                'timeout': 10}
        service = Service(dict)
        deftimeout = socket.getdefaulttimeout()
        service.getData('/tmp/socketresettest', '/cgi-bin/cmssw.cgi')
        assert deftimeout == socket.getdefaulttimeout()

if __name__ == '__main__':
    unittest.main()
