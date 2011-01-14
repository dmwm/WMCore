#!/usr/bin/env python
"""
_REST_t_

Unit tests for checking RESTModel works correctly

TODO: duplicate all direct call tests to ones that use HTTP
"""

import unittest
import cherrypy
import logging
import urllib2
import urllib
import json
from WMCore.Wrappers import JsonWrapper

from cherrypy import HTTPError
from wsgiref.handlers import format_date_time
from WMQuality.TestInit import TestInit
from WMCore.Configuration import Configuration
from WMCore.WebTools.Page import make_rfc_timestamp
from WMCore_t.WebTools_t.DummyRESTModel import DummyRESTModel
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig, cherrypySetup
from WMQuality.WebTools.RESTClientAPI import makeRequest, methodTest

dasConfig = DefaultConfig('WMCore_t.WebTools_t.DummyRESTModel')
dasConfig.setFormatter('WMCore.WebTools.DASRESTFormatter')

class RESTTest(RESTBaseUnitTest):
    def initialize(self):
        self.config = DefaultConfig('WMCore_t.WebTools_t.DummyRESTModel')
        do_debug = False

        if do_debug:
            self.config.Webtools.environment = 'development'
            self.config.Webtools.error_log_level = logging.DEBUG
            self.config.Webtools.access_log_level = logging.DEBUG
        else:
            self.config.Webtools.environment = 'production'
            self.config.Webtools.error_log_level = logging.WARNING
            self.config.Webtools.access_log_level = logging.WARNING
            
        self.urlbase = self.config.getServerUrl()
  
    def testUnsupportedFormat(self):
        # test not accepted type should return 406 error
        url = self.urlbase + 'ping'
        methodTest('GET', url, accept='text/das', output={'code':406})

    def testGoodEcho(self):
        verb ='POST'
        url = self.urlbase + 'echo'
        input_data={'message': 'unit test'}
        output={'code':200, 'type':'text/json',
              'data':'{"message": "unit test"}'}

        methodTest(verb, url, input_data, output=output)

    def testBadEchoWithPosArg(self):
        "Echo takes one argument (message), with the positional argument it should fail"
        verb ='POST'
        url = self.urlbase + 'echo/stuff'
        input_data={'message': 'unit test'}
        output={'code':400, 'type':'text/json'}
        methodTest(verb, url, input_data, output=output)
  
    def testBadMethodEcho(self):
        """
        The echo method isn't supported by GET, so should raise a 405
        """
        verb ='GET'
        url = self.urlbase + 'echo'
        input={'data': 'unit test'}
        output={'code':405, 'type':'text/json'}

        methodTest(verb, url, input, output=output)

    def testBadVerbEcho(self):
        "echo is only available to GET and POST, so should raise a 501"
        url = self.urlbase + 'echo'
        input={'data': 'unit test'}
        output={'code':501, 'type':'text/json'}

        for verb in ['DELETE', 'PUT']:
          methodTest(verb, url, input, output=output)

    def testPing(self):
        verb ='GET'
        url = self.urlbase + 'ping'
        output={'code':200, 'type':'text/json', 'data':'"ping"'}
        expireTime =3600

        methodTest(verb, url, output=output, expireTime=expireTime)

    def testBadPing(self):
        verb ='GET'

        url = self.urlbase + 'wrong'
        output={'code':404}
        methodTest(verb, url, output=output)

        url = self.urlbase + 'echo'
        output={'code':405}
        methodTest(verb, url, output=output)


        url = self.urlbase + 'ping/wrong'
        output={'code':400}
        methodTest(verb, url, output=output)

    def testException(self):
        """
        list takes a single integer argument, querying with a string
        """
        url = self.urlbase + 'list?int=a'
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, url)
        # urllib2,urlopen raise the error but not urllib.urlopen
        url = self.urlbase + 'list1?int=a'
        expected_data = {"exception": 400, "type": "HTTPError", "message": "Invalid input"}
        self.assertRaises(urllib2.HTTPError, urllib2.urlopen, url)
        urllib_data = urllib.urlopen(url)
        response_data = urllib_data.read()
        response_data = json.loads(response_data)
        self.assertEquals(response_data['type'], expected_data['type'])
        self.assertEquals(response_data['message'], expected_data['message'])
        self.assertEquals(urllib_data.getcode(), 400)
  
    @cherrypySetup(dasConfig)
    def testDasPing(self):
        verb ='GET'
        url = self.urlbase + 'ping'
        accept = 'text/json+das'
        output={'code':200, 'type':accept}
        expireTime =3600 

        data, expires = methodTest(verb, url, accept=accept, output=output, expireTime=expireTime)

        timestp = make_rfc_timestamp(expireTime)

        dict = JsonWrapper.loads(data) 
        response_expires = format_date_time(float(dict['response_expires']))
        self.assertEqual( response_expires ,  timestp, 'Expires DAS header incorrect (%s)' % response_expires )
        self.assertEqual( response_expires ,  expires, 'Expires DAS header incorrect (%s)' % response_expires )

        self.assertEqual( dict['results'] ,  'ping', 'got unexpected response %s' % dict['results'] )
      
    def testList(self):
        verb ='GET'
        url = self.urlbase + 'list/'
        request_input = {'input_int':123, 'input_str':'abc'}
        output={'code':200, 'type':'text/json'}
        result = json.loads(methodTest(verb, url, request_input=request_input, output=output)[0])
        for i in result.keys():
            self.assertEqual(result[i], request_input[i], '%s does not match response' % i)
        

    def testA(self):
        for t in ['GET', 'POST', 'PUT', 'DELETE', 'UPDATE']:
            response = makeRequest(url=self.urlbase + '/', values={'value':1234})
            assert response[1] == 200, 'Got a return code != 200 (got %s)' % response[1]

    def testSanitisePass(self):
        """
        Emulate how CherryPy passes arguments to a method, check that the data
        returned is correct.

        No server setup required
        """     
        drm = DummyRESTModel(self.config.getModelConfig())

        def func(*args, **kwargs):
           sanitised_input = drm._sanitise_input(args, kwargs, "list")
           return drm.list(**sanitised_input)

        # 2 positional args (e.g. url/arg1/arg2)
        result = func(123, 'abc')
        assert result == {'input_int':123, 'input_str':'abc'},\
                                'list with 2 positional args failed: %s' % result
        # 2 query string args (e.g. url?int=arg1&str=arg2)
        result = func(input_int=123, input_str='abc')
        assert result == {'input_int':123, 'input_str':'abc'},\
                               'list with 2 query string args failed: %s' % result
       
        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        result = func(123, input_str='abc')
        assert result == {'input_int':123, 'input_str':'abc'},\
               'list with 1 positional, 1 keyword failed: %s' % result
        
    def testSanitisePassHTTP(self):
        """
        Same as testSanitisePass but do it over http and check the returned http
        codes.
        """
        # 2 positional args (e.g. url/arg1/arg2)
        url = self.urlbase + 'list/123/abc'
        response = makeRequest(url=url)
        assert response[1] == 200, \
             'list with 2 positional args failed: ' +\
             '. Got a return code != 200 (got %s)' % response[1] +\
             '. Returned data: %s' % response[0]
              # 2 query string args (e.g. url?int=arg1&str=arg2)
        url = self.urlbase + 'list/'
        response = makeRequest(url=url, values={'input_int':'123', 'input_str':'abc'})
        assert response[1] == 200, \
                 'list with 2 query string args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0] 
     
        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        url = self.urlbase + 'list/123/'
        response = makeRequest(url=url, 
                                     values={'input_str':'abc'})
        assert response[1] == 200, \
                 'list with 1 positional, 1 keyword failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]
     
    def testSanitiseAssertFail(self):
        """
        No server set up required, the purpose of the test is just 
        demonstrating how validation is used.
        """
        drm = DummyRESTModel(self.config.getModelConfig())
     
        def func(*args, **kwargs):
            sanitised_input = drm._sanitise_input(args, kwargs, "list")
            return drm.list(**sanitised_input)
     
        # Wrong type for input args
        self.assertRaises(HTTPError, func, 123, 123)
        self.assertRaises(HTTPError, func, 'abc', 'abc')
        self.assertRaises(HTTPError, func, str = 123, int ='abc')
        self.assertRaises(HTTPError, func, str =' abc', int = 'abc')
        self.assertRaises(HTTPError, func, 'abc', 123)
        self.assertRaises(HTTPError, func, 'abc', 'abc')
        self.assertRaises(HTTPError, func, str = 123, int = 'abc')
        self.assertRaises(HTTPError, func, str =123, int = 123)
        self.assertRaises(HTTPError, func, str = 'abc', int ='abc')
     
        # Incorrect values for input args
        self.assertRaises(HTTPError, func, 1234, 'abc')
        self.assertRaises(HTTPError, func, 123, 'abcd')
     
        # Empty input data, when data is required
        self.assertRaises(HTTPError, func)
     
    def testSanitiseFailHTTP(self):
        """
        Same as testSanitisePass but do it over http and check the returned http
        codes.
        """
        # 2 positional args (e.g. url/arg1/arg2)
        url = self.urlbase + 'list/123/'
        response = makeRequest(url=url, accept='text/json')
        assert response[1] == 400, \
                 'list with 2 positional args failed: ' +\
                 '. Got a return code != 400 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]
     
        self.assertEqual(response[2], 'text/json', 'type is not text/json : %s' % type)
        # 2 query string args (e.g. url?int=arg1&str=arg2)
        url = self.urlbase + 'list'
        response = makeRequest(url=url, values={'int':'abc', 'str':'abc'})
        assert response[1] == 400, \
                 'list with 2 query string args failed: ' +\
                 '. Got a return code != 400 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0] 
     
        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        url = self.urlbase + 'list/abc'
        response = makeRequest(url=url, values={'str':'abc'})
        assert response[1] == 400, \
                 'list with 1 positional, 1 keyword failed: ' +\
                 '. Got a return code != 400 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]    
     
    # don't need server set up    
    def testDAOBased(self):
        drm = DummyRESTModel(self.config.getModelConfig())
     
        result = drm.methods['GET']['data1']['call']()
        self.assertEqual( result ,  123, 'Error default value is set to 123 but returns %s' % result )
     
        result =  drm.methods['GET']['data2']['call'](456)
        self.assertEqual( result['num'] ,  456 )
     
        result =  drm.methods['GET']['data2']['call'](num = 456)
        self.assertEqual( result['num'] ,  456 )
     
        result =  drm.methods['GET']['data3']['call'](num = 456, thing="TEST")
        self.assertEqual( result['num'] == 456 and result['thing'] ,  "TEST" )
     
    def testDAOBasedHTTP(self):
        """
        Same as testSanitisePass but do it over http and check the returned http
        codes.
        """
        # 2 positional args (e.g. url/arg1/arg2)
        url = self.urlbase + 'data1/'
        response = makeRequest(url=url)
        assert response[1] == 200, \
                 'dao without args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]
        self.assertEqual( response[0] ,  '123', response[0])
     
        # 2 query string args (e.g. url?int=arg1&str=arg2)
        url = self.urlbase + 'data2'
        response = makeRequest(url=url, values={'num':456})
        
        assert response[1] == 200, \
                 'dao with 1 args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0] 
        #Warning quotation type matters
        #Should use encoded and decoded format
        self.assertEqual( response[0] ,  "{'num': '456'}", "should be {'num': '456'} but got %s" % response[0]          )
     
        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        url = self.urlbase + 'data3/123'
        response = makeRequest(url=url, values={'thing':'abc'})
        
        assert response[1] == 200, \
                 'dao with 1 positional, 1 keyword failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]
        #Warning quotation type and order matters
        #Should use encoded and decoded format
        self.assertEqual( response[0] ,  "{'thing': 'abc', 'num': '123'}", "should be {'thing': 'abc', 'num': '123'} but got %s" % response[0] )
     
    def testListTypeArgs(self):
        # 2 positional args (e.g. url/arg1/arg2)
        url = self.urlbase + 'listTypeArgs?aList=1'
        response = makeRequest(url=url)
        assert response[1] == 200 and response[0] == "[1]", \
                 'list args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]
     
     
        # 2 values with the same keywords (e.g. url/arg1/arg2)
        url = self.urlbase + 'listTypeArgs?aList=1&aList=2'
        response = makeRequest(url=url)
        assert response[1] == 200 and response[0] == "[1, 2]", \
                 'list args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]
     
 
if __name__ == "__main__":
    unittest.main() 
