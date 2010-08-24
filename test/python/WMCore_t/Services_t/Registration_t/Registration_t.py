#!/usr/bin/env python
'''
Created on 16 Jul 2009

@author: metson
'''

import unittest
import logging
import os

from WMCore.Services.Registration.Registration import Registration
from WMCore.Services.Requests import JSONRequests

class RegistrationTest(unittest.TestCase):
    """
    Provide setUp and tearDown for Reader package module
    
    """
    def setUp(self):
        """
        setUP global values
        """
        pass
        
    def testPush(self):
        reg_info ={
                   "url": "https://globaldbs",
                   "admin": "joe.bloggs@cern.ch",
                   "type": "DBS",
                   "name": "Global DBS",
                   "timeout": 2
        }
        
        reg = Registration({'inputdata': reg_info,
                            'endpoint': 'http://localhost:5984/registrationservice/',
                            'cert': os.getcwd() + '/' +__file__,
                            'key': os.getcwd() + '/' +__file__})
        f = reg.refreshCache()
        f.read()
        f.close()
        
        json = JSONRequests('localhost:5984')
        data = json.get('/registrationservice/' + \
                       str(reg['inputdata']['url'].__hash__()))
        for k, v in reg_info.items():
            if k != 'timestamp':
                assert data[0][k] == v, \
                "Registration incomplete: %s should equal %s for key %s" % (data[0][k], v, k) 
        
if __name__ == '__main__':
    unittest.main()        