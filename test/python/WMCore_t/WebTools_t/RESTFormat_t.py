#!/usr/bin/env python
"""
_REST_t_

Unit tests for checking RESTModel works correctly

TODO: duplicate all direct call tests to ones that use HTTP
"""




import unittest
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
import urllib
import urllib2

from cherrypy import HTTPError
from WMCore.WebTools.RESTFormatter import RESTFormatter
from WMCore_t.WebTools_t.DummyRESTModel import DummyRESTModel

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTClientAPI import methodTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMCore.Wrappers import JsonWrapper

class RESTFormatTest(RESTBaseUnitTest):
    
    def initialize(self):
        self.config = DefaultConfig('WMCore_t.WebTools_t.DummyRESTModel')        
        self.urlbase = self.config.getServerUrl()
    
    def testUnsupportedFormat(self):
        
        # test not accepted type should return 406 error
        url = self.urlbase +'list1/'
        methodTest('GET', url, accept='text/das', output={'code':406})
    
    def testSupportedFormat(self):
    
        rf = RESTFormatter(config=self.config.Webtools)
        url = self.urlbase +'list1/'
        
        for type in rf.supporttypes.keys():
            # test accepted type should return 200 error
            methodTest('GET', url, accept=type, output={'code':200})
    
    def testEncodedInput(self):
        
        type = 'text/plain'
       
        url = self.urlbase + 'list3?a=a%&b=b'
        print url
        methodTest('GET', url, accept=type, 
                         output={'code':200, 'data':"{'a': 'a%', 'b': 'b'}"})
       
        input={'a':'%', 'b':'b'}  
           
        #methodTest encoded input with urlencode
        url = self.urlbase +'list3'
        methodTest('GET', url, accept=type, input=input, 
                         output={'code':200, 'data':"{'a': '%', 'b': 'b'}"})
        
       
        # This is not supported: commented out for now in case it will get supported later
        # auto parameter parsing on certain content type
#        url = self.urlbase +'list3'
#        data = JsonWrapper.dumps(input)
#        methodTest('POST', url, accept=type, input=data,
#                         contentType="text/json",
#                         output={'code':200, 'data':"{'a': '%', 'b': 'b'}"}
#                         )
#       
        
        input={'a':'%', 'b':'b'}
        url = self.urlbase +'list3'  
        methodTest('POST', url, accept=type, input=input,
                         output={'code':200, 'data':"{'a': '%', 'b': 'b'}"}
                         )
    
    def testReturnFormat(self):
        
        type = 'application/json'
        
        url = self.urlbase +'list3?a=a%&b=b'
        methodTest('GET', url, accept=type, 
                         output={'code':200, 'data':'{"a": "a%", "b": "b"}'})
       
        url = self.urlbase + 'list1?int=a'
        try:
            urllib.urlopen(url)
            #urllib2.urlopen(url)
        except urllib2.HTTPError, h:
            print "Exception got cought %s" % h.read()
        
        # urllib2,urlopen raise the error but not urllib.urlopen
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, url)
        
        methodTest('GET', url, accept=type, 
                         output={'code':400, 
                                 'data':"""{"exception": 400, "type": "HTTPError", "message": "list1() got an unexpected keyword argument 'int'"}"""})
        url = self.urlbase + 'list?int=a&str=a'
        methodTest('GET', url, accept=type,
                         output={'code':400, 
                                 'data':"""{"exception": 400, "type": "HTTPError", "message": "val_1 failed: <type 'str'> not int"}"""})
       
    def testException(self):
        
        import urllib2
        url = self.urlbase + 'list1?int=a'
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, url)
        
        #TODO check urllib.open is raising HTTPError
        #import urllib
        #self.assertRaises(urllib2.HTTPError, urllib.urlopen, url)
        
if __name__ == "__main__":
    unittest.main() 
        
