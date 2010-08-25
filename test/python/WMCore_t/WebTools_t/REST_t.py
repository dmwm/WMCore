#!/usr/bin/env python
"""
_REST_t_

Unit tests for checking RESTModel works correctly

TODO: duplicate all direct call tests to ones that use HTTP
"""

__revision__ = "$Id: REST_t.py,v 1.16 2010/01/05 20:53:36 sryu Exp $"
__version__ = "$Revision: 1.16 $"

import unittest
import json
from cherrypy import HTTPError
from wsgiref.handlers import format_date_time
from WMQuality.TestInit import TestInit
from WMCore.Configuration import Configuration
from WMCore.WebTools.Page import make_rfc_timestamp
from DummyRESTModel import DummyRESTModel
#decorator import for RESTServer setup
from RESTServerSetup import setUpDummyRESTModel, setUpDAS, serverSetup 
from RESTServerSetup import makeRequest

class RESTTest(unittest.TestCase):
    
    def setUp(self):
        self.dasFlag = False
        self.restModel = 'WMCore.WebTools.RESTModel'
    
    def tearDown(self):
        self.dasFlag = None
        self.restModel = None
    
    def methodTest(self, verb, url, input={}, accept='text/json', output={} , expireTime=300):
        
        data, code, type, response = makeRequest(url, input, verb, accept)
        
        keyMap = {'code': code, 'data': data, 'type': type, 'response': response}
        for key, value in output.items():
            assert keyMap[key] == value, \
                'Got a return %s != %s (got %s) (data %s)' % (key, value, code, data)
        
        expires = response.getheader('Expires')        
        assert expires == make_rfc_timestamp(expireTime), 'Expires header incorrect (%s)' % expires
        
        return data, expires
            
    @serverSetup
    def testUnsupportedFormat(self):
        
        # test not accepted type should return 406 error
        self.methodTest('GET', '/rest/ping/', accept='text/das', output={'code':406})
                
    @serverSetup
    def testGoodEcho(self):
        
        verb ='POST'
        url ='/rest/echo'
        input={'data': 'unit test'}
        output={'code':200, 'type':'text/json',
                'data':'{"args": [], "kwargs": {"data": "unit test"}}'}
        
        self.methodTest(verb, url, input, output=output)
        
    @serverSetup
    def testGoodEchoWithPosArg(self):
        
        verb ='POST'
        url ='/rest/echo/stuff'
        input={'data': 'unit test'}
        output={'code':200, 'type':'text/json',
                'data':'{"args": ["stuff"], "kwargs": {"data": "unit test"}}'}
        
        self.methodTest(verb, url, input, output=output)
        
    @serverSetup        
    def testBadMethodEcho(self):
        
        verb ='GET'
        url ='/rest/echo'
        input={'data': 'unit test'}
        output={'code':405, 'type':'text/json'}
        
        self.methodTest(verb, url, input, output=output)
        
    @serverSetup      
    def testBadVerbEcho(self):
        url ='/rest/echo'
        input={'data': 'unit test'}
        output={'code':501, 'type':'text/json'}
        
        for verb in ['DELETE', 'PUT']:
            self.methodTest(verb, url, input, output=output)
        
    @serverSetup
    def testPing(self):
        
        verb ='GET'
        url ='/rest/ping'
        output={'code':200, 'type':'text/json', 'data':'"ping"'}
        expireTime =3600
        
        self.methodTest(verb, url, output=output, expireTime=expireTime)
        
    @serverSetup
    def testBadPing(self):
       
        verb ='GET'
        
        url ='/rest/wrong'
        output={'code':404}
        self.methodTest(verb, url, output=output)
        
        url ='/rest/echo'
        output={'code':405}
        self.methodTest(verb, url, output=output)
        
        
        url ='/rest/ping/wrong'
        output={'code':400}
        self.methodTest(verb, url, output=output)
        
        
    @setUpDAS
    @serverSetup    
    def testDasPing(self, das=True):
        
        verb ='GET'
        url ='/rest/ping'
        accept = 'text/json+das'
        output={'code':200, 'type':accept}
        expireTime =3600 
        
        data, expires = self.methodTest(verb, url, accept=accept, output=output, expireTime=expireTime)
        
        timestp = make_rfc_timestamp(expireTime)
        
        dict = json.loads(data) 
        response_expires = format_date_time(float(dict['response_expires']))
        assert response_expires == timestp, 'Expires DAS header incorrect (%s)' % response_expires
        assert response_expires == expires, 'Expires DAS header incorrect (%s)' % response_expires
        
        assert dict['results'] == 'ping', 'got unexpected response %s' % dict['results']
    
    @setUpDummyRESTModel    
    @serverSetup
    def testList(self):
        
        verb ='GET'
        url ='/rest/list/'
        input = {'int':123, 'str':'abc'}
        output={'code':200, 'type':'text/json', 'data':'{"int": 123, "str": "abc"}'}
        self.methodTest(verb, url, input=input, output=output)
        
        
    @serverSetup    
    def testA(self):
        for t in ['GET', 'POST', 'PUT', 'DELETE', 'UPDATE']:
                response = makeRequest(values={'value':1234})
                assert response[1] == 200, \
                 'Got a return code != 200 (got %s)' % response[1]
    
    def testSanitisePass(self):
        """
        Emulate how CherryPy passes arguments to a method, check that the data
        returned is correct.
        """
        config = Configuration()
        component = config.component_('UnitTests')
        component.application = 'UnitTests'
        component.database = 'sqlite://'
                
        drm = DummyRESTModel(component)
        
        # 2 positional args (e.g. url/arg1/arg2)
        result = drm.list(123, 'abc')
        assert result == {'int':123, 'str':'abc'},\
                'list with 2 positional args failed: %s' % result
        # 2 query string args (e.g. url?int=arg1&str=arg2)
        result = drm.list(int=123, str='abc')
        assert result == {'int':123, 'str':'abc'},\
                'list with 2 query string args failed: %s' % result
        
        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        result = drm.list(123, str='abc')
        assert result == {'int':123, 'str':'abc'},\
                'list with 1 positional, 1 keyword failed: %s' % result
#    
    @setUpDummyRESTModel
    @serverSetup
    def testSanitisePassHTTP(self):
        """
        Same as testSanitisePass but do it over http and check the returned http
        codes.
        """
        #TODO when url is /list/,,,, it returns success. should fail correct url is /rest/list...
        # also error message is html format although accept type is text/json
        # 2 positional args (e.g. url/arg1/arg2)
#        response = makeRequest(uri='/list/123/abc', accept='text/json')
#        assert response[1] != 200, \
#                'list with 2 positional args failed: ' +\
#                '. Got a return code != 200 (got %s)' % response[1] +\
#                '. Returned data: %s' % response[0]
                 
        # 2 positional args (e.g. url/arg1/arg2)
        response = makeRequest(uri='/rest/list/123/abc')
        assert response[1] == 200, \
                'list with 2 positional args failed: ' +\
                '. Got a return code != 200 (got %s)' % response[1] +\
                '. Returned data: %s' % response[0]
                 
        # 2 query string args (e.g. url?int=arg1&str=arg2)
        response = makeRequest(uri='/rest/list', 
                                    values={'int':'123', 'str':'abc'})
        assert response[1] == 200, \
                'list with 2 query string args failed: ' +\
                '. Got a return code != 200 (got %s)' % response[1] +\
                '. Returned data: %s' % response[0] 
        
        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        response = makeRequest(uri='/rest/list/123/', 
                                    values={'str':'abc'})
        assert response[1] == 200, \
                'list with 1 positional, 1 keyword failed: ' +\
                '. Got a return code != 200 (got %s)' % response[1] +\
                '. Returned data: %s' % response[0]
    
                 
    def testSanitiseAssertFail(self):
        
        config = Configuration()
        component = config.component_('UnitTests')
        component.application = 'UnitTests'
        component.database = 'sqlite://'
                
        drm = DummyRESTModel(component)
        
        #TODO: this is not really testing where is fails.
        #However the purpose of the test is just demonstrating how validation is used,
        #If necessary make the test more accurate.
         
        # Wrong type for input args
        self.assertRaises(HTTPError, drm.list, 123, 123)
        self.assertRaises(HTTPError, drm.list, 'abc', 'abc')
        self.assertRaises(HTTPError, drm.list, str = 123, int ='abc')
        self.assertRaises(HTTPError, drm.list, str =' abc', int = 'abc')
        self.assertRaises(HTTPError, drm.list, 'abc', 123)
        self.assertRaises(HTTPError, drm.list, 'abc', 'abc')
        self.assertRaises(HTTPError, drm.list, str = 123, int = 'abc')
        self.assertRaises(HTTPError, drm.list, str =123, int = 123)
        self.assertRaises(HTTPError, drm.list, str = 'abc', int ='abc')
        
        # Incorrect values for input args
        self.assertRaises(HTTPError, drm.list, 1234, 'abc')
        self.assertRaises(HTTPError, drm.list, 123, 'abcd')
        
        # Empty input data, when data is required
        self.assertRaises(HTTPError, drm.list)
    

    @setUpDummyRESTModel
    @serverSetup
    def testSanitiseFailHTTP(self):
        """
        Same as testSanitisePass but do it over http and check the returned http
        codes.
        """
        
        # 2 positional args (e.g. url/arg1/arg2)
        response = makeRequest(uri='/rest/list/123/', accept='text/json')
        assert response[1] == 404, \
                'list with 2 positional args failed: ' +\
                '. Got a return code != 400 (got %s)' % response[1] +\
                '. Returned data: %s' % response[0]
        
        assert response[2] == 'text/json', 'type is not text/json : %s' % type         
        # 2 query string args (e.g. url?int=arg1&str=arg2)
        response = makeRequest(uri='/rest/list', 
                                    values={'int':'abc', 'str':'abc'})
        assert response[1] == 400, \
                'list with 2 query string args failed: ' +\
                '. Got a return code != 400 (got %s)' % response[1] +\
                '. Returned data: %s' % response[0] 
        
        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        response = makeRequest(uri='/rest/list/abc/', 
                                    values={'str':'abc'})
        assert response[1] == 400, \
                'list with 1 positional, 1 keyword failed: ' +\
                '. Got a return code != 400 (got %s)' % response[1] +\
                '. Returned data: %s' % response[0]    

        
    def testDAOBased(self):
        config = Configuration()
        component = config.component_('UnitTests')
        component.application = 'UnitTests'
        component.database = 'sqlite://'
                
        drm = DummyRESTModel(component)
        
        result = drm.methods['GET']['data1']['call']()
        assert result == 123, 'Error default value is set to 123 but returns %s' % result
        
        result =  drm.methods['GET']['data2']['call'](456)
        assert result['num'] == 456
        
        result =  drm.methods['GET']['data2']['call'](num = 456)
        assert result['num'] == 456
        
        result =  drm.methods['GET']['data3']['call'](num = 456, thing="TEST")
        assert result['num'] == 456 and result['thing'] == "TEST"
        
    
    @setUpDummyRESTModel
    @serverSetup
    def testDAOBasedHTTP(self):
        """
        Same as testSanitisePass but do it over http and check the returned http
        codes.
        """
        
        # 2 positional args (e.g. url/arg1/arg2)
        response = makeRequest(uri='/rest/data1/')
        assert response[1] == 200, \
                'dao without args failed: ' +\
                '. Got a return code != 200 (got %s)' % response[1] +\
                '. Returned data: %s' % response[0]
        assert response[0] == '123', response[0]        
        
        # 2 query string args (e.g. url?int=arg1&str=arg2)
        response = makeRequest(uri='/rest/data2', 
                                    values={'num':456})
        assert response[1] == 200, \
                'dao with 1 args failed: ' +\
                '. Got a return code != 200 (got %s)' % response[1] +\
                '. Returned data: %s' % response[0] 
        #Warning quotation type matters
        #Should use encoded and decoded format
        assert response[0] == "{'num': '456'}", "should be {'num': '456'} but got %s" % response[0]         
        
        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        response = makeRequest(uri='/rest/data3/123/', 
                                    values={'thing':'abc'})
        assert response[1] == 200, \
                'dao with 1 positional, 1 keyword failed: ' +\
                '. Got a return code != 200 (got %s)' % response[1] +\
                '. Returned data: %s' % response[0]
        #Warning quotation type and order matters
        #Should use encoded and decoded format
        assert response[0] == "{'thing': 'abc', 'num': '123'}", "should be {'thing': 'abc', 'num': '123'} but got %s" % response[0]         
        
if __name__ == "__main__":
    unittest.main() 
