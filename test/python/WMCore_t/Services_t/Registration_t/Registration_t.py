#!/usr/bin/env python
'''
Created on 16 Jul 2009

@author: metson
'''

import unittest
import logging
import os
import tempfile

from WMCore.Services.Registration.Registration import Registration
from WMCore.Services.Requests import BasicAuthJSONRequests
from WMQuality.TestInitCouchApp import TestInitCouchApp

class RegistrationTest(unittest.TestCase):
    """
    Provide setUp and tearDown for Reader package module
    
    """
    def setUp(self):
        """
        setUP global values
        """
        testname = self.id().split('.')[-1]
        
        logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='service_unittests.log',
                    filemode='w')
        
        logger_name = 'Service%s' % testname.replace('test', '', 1)
        
        self.logger = logging.getLogger(logger_name)
        
        self.cache_path = tempfile.mkdtemp()
        self.testInit = TestInitCouchApp("RegistrationTest")
        self.testInit.setupCouch("regsvc")

    def tearDown(self):
        self.testInit.tearDownCouch()
        
    def testPush(self):
        reg_info ={
                   "url": "https://globaldbs",
                   "admin": "joe.bloggs@cern.ch",
                   "type": "DBS",
                   "name": "Global DBS",
                   "timeout": 2
        }
        
        reg = Registration({'inputdata': reg_info,
                            'endpoint': '%s/regsvc/' % self.testInit.couchUrl,
                            'cert': os.getcwd() + '/' +__file__,
                            'key': os.getcwd() + '/' +__file__,
                            'logger': self.logger, 
                            'cachepath' : self.cache_path,
                            'req_cache_path': '%s/requests' % self.cache_path,})
        reg.refreshCache()
        
        json = BasicAuthJSONRequests(self.testInit.couchUrl)
        data = json.get('/regsvc/' + \
                       str(reg['inputdata']['url'].__hash__()))
        for k, v in reg_info.items():
            if k != 'timestamp':
                assert data[0][k] == v, \
                "Registration incomplete: %s should equal %s for key %s" % (data[0][k], v, k) 
        
if __name__ == '__main__':
    unittest.main()        