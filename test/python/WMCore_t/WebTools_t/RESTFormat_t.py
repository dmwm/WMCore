#!/usr/bin/env python
"""
_REST_t_

Unit tests for checking RESTModel works correctly

TODO: duplicate all direct call tests to ones that use HTTP
"""

__revision__ = "$Id: RESTFormat_t.py,v 1.3 2010/01/11 16:36:07 sryu Exp $"
__version__ = "$Revision: 1.3 $"

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
        
        type = 'text/plain'
        
        methodTest('GET', '/rest/list3?a=a%&b=b', accept=type, 
                         output={'code':200, 'data':"{'a': 'a%', 'b': 'b'}"})
       
        input={'a':'%', 'b':'b'}  
           
        #methodTest encoded input with urlencode
        methodTest('GET', '/rest/list3', accept=type, input=input, 
                         output={'code':200, 'data':"{'a': '%', 'b': 'b'}"})
        
        
        # fix this
#        methodTest('POST', '/rest/list3', accept=type, input=input,
#                         contentType="application/json",
#                         output={'code':200, 'data':"{'a': '%', 'b': 'b'}"}
#                         )
        
        input={'a':'%', 'b':'b'}  
        methodTest('POST', '/rest/list3', accept=type, input=input,
                         output={'code':200, 'data':"{'a': '%', 'b': 'b'}"}
                         )
    
    @serverSetup
    def testReturnFormat(self):
        type = 'application/json'
        
        methodTest('GET', '/rest/list3?a=a%&b=b', accept=type, 
                         output={'code':200, 'data':'{"a": "a%", "b": "b"}'})
       
        
        methodTest('GET', '/rest/list1?int=a', accept=type, 
                         output={'code':400, 
                                 'data':"""{"exception": 400, "type": "HTTPError", "message": "list1() got an unexpected keyword argument 'int'"}"""})
        
        methodTest('GET', '/rest/list?int=a&str=a', accept=type,
                         output={'code':400, 
                                 'data':"""{"exception": 400, "type": "HTTPError", "message": {"AssertionError": "val_1 failed: <type 'str'> not int"}}"""})
       
       
if __name__ == "__main__":
    unittest.main() 
        