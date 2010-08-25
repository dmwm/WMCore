#!/usr/bin/env python
"""
_REST_t_

Unit tests for checking RESTModel works correctly

TODO: duplicate all direct call tests to ones that use HTTP
"""

__revision__ = "$Id: RESTFormat_t.py,v 1.1 2010/01/06 20:57:38 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import json
from cherrypy import HTTPError
from WMCore.WebTools.RESTFormatter import RESTFormatter
from DummyRESTModel import DummyRESTModel
#decorator import for RESTServer setup
from RESTServerSetup import setUpDummyRESTModel, setUpDAS, serverSetup, getDefaultConfig
from RESTClientAPI import makeRequest, methodTest
import urllib

class RESTFormatTest(unittest.TestCase):
    
    def setUp(self):
        self.dasFlag = False
        self.restModel = 'DummyRESTModel'
    
    def tearDown(self):
        self.dasFlag = None
        self.restModel = None
            
    @serverSetup
    def testUnsupportedFormat(self):
        
        # test not accepted type should return 406 error
        methodTest('GET', '/rest/list1/', accept='text/das', output={'code':406})
    
    @serverSetup    
    def testSupportedFormat(self):
        dummycfg = getDefaultConfig()
        rf = RESTFormatter(config=dummycfg.Webtools)
        
        for type in rf.supporttypes.keys():
            # test accepted type should return 200 error
            methodTest('GET', '/rest/list1/', accept=type, output={'code':200})
    
    @serverSetup    
    def testEncodedInput(self):
        
        #for type in ['application/json', 'application/x-www-form-urlencoded'] :
            # test not accepted type should return 406 error
        #type = 'application/x-www-form-urlencoded'
        type = 'text/plain'
        input={'a':'%', 'b':'b'}    
        #input = urllib.urlencode(input)
        methodTest('GET', '/rest/list3', accept=type, input=input, 
                         output={'code':200, 'data':"{'a': '%', 'b': 'b'}"})
        
        methodTest('GET', '/rest/list3?a=a%&b=b', accept=type, 
                         output={'code':200, 'data':"{'a': 'a%', 'b': 'b'}"})
            
if __name__ == "__main__":
    unittest.main() 
        