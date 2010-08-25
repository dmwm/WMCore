#!/usr/bin/env python
"""
_REST_t_

Unit tests for checking RESTModel works correctly

TODO: duplicate all direct call tests to ones that use HTTP
"""

__revision__ = "$Id: REST_t.py,v 1.15 2010/01/04 17:04:49 sryu Exp $"
__version__ = "$Revision: 1.15 $"

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
    
    @serverSetup
    def testUnsupportedFormat(self):
        
        for method in ['GET']:
            data, code, type, response = makeRequest('/rest/ping/', 
                                                   {}, 
                                                   method, 'text/das')
            assert code == 406, \
                'Got a return code != 406 (got %s) (data %s)' % (code, data)
                
    @serverSetup
    def testGoodEcho(self):
        
        for method in ['POST']:
            data, code, type, response = makeRequest('/rest/echo', 
                                                   {'data': 'unit test'}, 
                                                   method, 'text/json')
            assert code == 200, \
                'Got a return code != 200 (got %s)' % code
            expires = response.getheader('Expires')
            assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
            assert type == 'text/json'
            assert data == '{"args": [], "kwargs": {"data": "unit test"}}', 'got unexpected response %s' % data
    
    @serverSetup
    def testGoodEchoWithPosArg(self):
        
        for method in ['POST']:
            data, code, type, response = makeRequest('/rest/echo/stuff', 
                                                {'data': 'unit test'},
                                                method, 
                                                'text/json')
            assert code == 200, \
                'Got a return code != 200 (got %s)' % code
            expires = response.getheader('Expires')
            assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
            assert type == 'text/json'
            assert data == '{"args": ["stuff"], "kwargs": {"data": "unit test"}}', 'got unexpected response %s' % data
    
    @serverSetup        
    def testBadMethodEcho(self):
        
        for method in ['GET']:
            data, code, type, response = makeRequest('/rest/echo', {'data': 'unit test'}, 
                                          method, 'text/json')
            assert int(code) == 405, "Didn't get a 'Method Not Allowed' response for %s (got %s)" % (method, code)
            expires = response.getheader('Expires')
            assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
            assert type == 'text/json' 
    
    @serverSetup      
    def testBadVerbEcho(self):
    
        for method in ['DELETE', 'PUT']:
            data, code, type, response = makeRequest('/rest/echo', {'data': 'unit test'}, 
                                          method, 'text/json')
            assert int(code) == 501, "Didn't get a 'Not Implemented' response for %s (got %s), message: %s" % (method, code, data)
            expires = response.getheader('Expires')
            assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
            assert type == 'text/json'
    
    @serverSetup
    def testPing(self):
        
        data, code, type, response = makeRequest('/rest/ping/',
                                          type='GET', accept='text/json')
        assert code == 200, 'Got a return code != 200 (got %s), message: %s' % (code, data)
        assert type == 'text/json'
        expires = response.getheader('Expires')
        assert expires == make_rfc_timestamp(3600), 'Expires header incorrect (%s)' % expires 
        assert data == '"ping"', 'got unexpected response %s' % data

    @serverSetup
    def testBadPing(self):
       
        #data, code, type, response = makeRequest('/wrong/',
        #                                  type='GET', accept='text/json')
        #assert code == 405, 'Got a return code != 405 (got %s), message: %s' % (code, data)
        data, code, type, response = makeRequest('/rest/wrong/',
                                          type='GET', accept='text/json')
        assert code == 404, 'Got a return code != 404 (got %s), message: %s' % (code, data)
        
        data, code, type, response = makeRequest('/rest/echo/',
                                          type='GET', accept='text/json')
        assert code == 405, 'Got a return code != 405 (got %s), message: %s' % (code, data)
        
        data, code, type, response = makeRequest('/rest/ping/wrong/',
                                          type='GET', accept='text/json')
        
        assert code == 400, 'Got a return code != 400 (got %s), message: %s' % (code, data)
        assert type == 'text/json'
        expires = response.getheader('Expires')
        assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires 
    
    @setUpDAS
    @serverSetup    
    def testDasPing(self, das=True):
        
        data, code, type, response = makeRequest('/rest/ping',
                                          type='GET', accept='text/json+das')
        assert code == 200, 'Got a return code != 200 (got %s), message: %s' % (code, data)
        assert type == 'text/json+das'
        expires = response.getheader('Expires')
        timestp = make_rfc_timestamp(3600)
        dict = json.loads(data) 
        response_expires = format_date_time(float(dict['response_expires']))
        assert expires == timestp, 'Expires header incorrect (%s)' % expires
        assert response_expires == timestp, 'Expires DAS header incorrect (%s)' % response_expires
        assert response_expires == expires, 'Expires DAS header incorrect (%s)' % response_expires
        
        assert dict['results'] == 'ping', 'got unexpected response %s' % dict['results']
    
    @setUpDummyRESTModel    
    @serverSetup
    def testList(self):
        
        data, code, type, response = makeRequest('/rest/list/', {'int':123, 'str':'abc'},
                                          type='GET', accept='text/json')
        assert code == 200, 'Got a return code != 200 (got %s), message: %s' % (code, data)
        assert type == 'text/json', 'type is not text/json : %s' % type
        expires = response.getheader('Expires')
        assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
        assert data == '{"int": 123, "str": "abc"}', 'data is not correct %s' % data
    
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
        
        print '**********'
        print response[2]
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
