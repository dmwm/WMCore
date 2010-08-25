#!/usr/bin/env python
"""
_Trigger_t_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: REST_t.py,v 1.8 2009/12/28 21:37:06 sryu Exp $"
__version__ = "$Revision: 1.8 $"

import unittest
import os
import threading
import logging 
import cherrypy
import json
import urllib, urllib2
from httplib import HTTPConnection
from cherrypy import HTTPError
from wsgiref.handlers import format_date_time
from WMQuality.TestInit import TestInit
from WMCore.Configuration import Configuration
from WMCore.WebTools.Root import Root
from WMCore.WebTools.Page import make_rfc_timestamp
from DummyRESTModel import DummyRESTModel
        
class RESTTest(unittest.TestCase):
    
    def configureServer(self, restModel='WMCore.WebTools.RESTModel', das=False):
        dummycfg = Configuration()
        dummycfg.component_('Webtools')
        dummycfg.Webtools.application = 'UnitTests'
        dummycfg.Webtools.log_screen = False
        dummycfg.Webtools.access_file = '/dev/null'
        dummycfg.Webtools.error_file = '/dev/null'
        dummycfg.component_('UnitTests')
        dummycfg.UnitTests.title = 'CMS WMCore/WebTools Unit Tests'
        dummycfg.UnitTests.description = 'Dummy server for the running of unit tests' 
        dummycfg.UnitTests.section_('views')
        
        active = dummycfg.UnitTests.views.section_('active')
        active.section_('rest')
        active.rest.object = 'WMCore.WebTools.RESTApi'
        active.rest.templates = '/tmp'
        active.rest.database = 'sqlite://'
        active.rest.section_('model')
        active.rest.model.object = restModel
        active.rest.section_('formatter')
        if das:
            active.rest.formatter.object = 'WMCore.WebTools.DASRESTFormatter'
        else:
            active.rest.formatter.object = 'WMCore.WebTools.RESTFormatter'
        active.rest.formatter.templates = '/tmp'
        
        rt = Root(dummycfg)
        return rt
    
    def testGoodEcho(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        
        for method in ['POST']:
            data, code, type, response = self.makeRequest('/rest/echo', 
                                                   {'data': 'unit test'}, 
                                                   method, 'text/json')
            assert code == 200, \
                'Got a return code != 200 (got %s)' % code
            expires = response.getheader('Expires')
            assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
            assert type == 'text/json'
            assert data == '{"args": [], "kwargs": {"data": "unit test"}}', 'got unexpected response %s' % data
    
    def testGoodEchoWithPosArg(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        for method in ['POST']:
            data, code, type, response = self.makeRequest('/rest/echo/stuff', 
                                                {'data': 'unit test'},
                                                method, 
                                                'text/json')
            assert code == 200, \
                'Got a return code != 200 (got %s)' % code
            expires = response.getheader('Expires')
            assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
            assert type == 'text/json'
            assert data == '{"args": ["stuff"], "kwargs": {"data": "unit test"}}', 'got unexpected response %s' % data
            
    def testBadMethodEcho(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        
        for method in ['GET']:
            data, code, type, response = self.makeRequest('/rest/echo', {'data': 'unit test'}, 
                                          method, 'text/json')
            assert int(code) == 405, "Didn't get a 'Method Not Allowed' response for %s (got %s)" % (method, code)
            expires = response.getheader('Expires')
            assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
            assert type == 'text/json' 
        rt.stop()
          
    def testBadVerbEcho(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        
        for method in ['DELETE', 'PUT']:
            data, code, type, response = self.makeRequest('/rest/echo', {'data': 'unit test'}, 
                                          method, 'text/json')
            assert int(code) == 501, "Didn't get a 'Not Implemented' response for %s (got %s), message: %s" % (method, code, data)
            expires = response.getheader('Expires')
            assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
            assert type == 'text/json'
        rt.stop()
    
    def testPing(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        data, code, type, response = self.makeRequest('/rest/ping/',
                                          type='GET', accept='text/json')
        assert code == 200, 'Got a return code != 200 (got %s), message: %s' % (code, data)
        assert type == 'text/json'
        expires = response.getheader('Expires')
        assert expires == make_rfc_timestamp(3600), 'Expires header incorrect (%s)' % expires 
        assert data == '"ping"', 'got unexpected response %s' % data
        
        rt.stop()
        
    def testBadPing(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        data, code, type, response = self.makeRequest('/rest/ping/wrong/',
                                          type='GET', accept='text/json')
        assert code == 400, 'Got a return code != 400 (got %s), message: %s' % (code, data)
        assert type == 'text/json'
        expires = response.getheader('Expires')
        assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires 

        rt.stop()
        
    def testDasPing(self):
        rt = self.configureServer(das=True)
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        data, code, type, response = self.makeRequest('/rest/ping',
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
        
        rt.stop()
    
    def testList(self):
        rt = self.configureServer(restModel='DummyRESTModel')
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        data, code, type, response = self.makeRequest('/rest/list/', {'int':123, 'str':'abc'},
                                          type='GET', accept='text/json')
        assert code == 200, 'Got a return code != 200 (got %s), message: %s' % (code, data)
        assert type == 'text/json'
        expires = response.getheader('Expires')
        assert expires == make_rfc_timestamp(300), 'Expires header incorrect (%s)' % expires
        assert data == '{"int": 123, "str": "abc"}', 'data is not correct %s' % data
        
    def makeRequest(self, uri='/rest/', values=None, type='GET', accept="text/plain"):
        headers = {}
        headers = {"Content-type": "application/x-www-form-urlencoded",
                   "Accept": accept}
        data = None
        if values:
            data = urllib.urlencode(values)
        if type != 'POST' and data != None:
            uri = '%s?%s' % (uri, data)
        conn = HTTPConnection('localhost:8080')
        conn.connect()
        conn.request(type, uri, data, headers)
        response = conn.getresponse()
        
        data = response.read()
        conn.close()
        type = response.getheader('content-type').split(';')[0]
        return data, response.status, type, response
    
    def testA(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        for t in ['GET', 'POST', 'PUT', 'DELETE', 'UPDATE']:
                response = self.makeRequest(values={'value':1234})
                assert response[1] == 200, \
                 'Got a return code != 200 (got %s)' % response[1]
        
        rt.stop()
        
    def testSanitisePass(self):
        config = Configuration()
        component = config.component_('UnitTests')
        component.application = 'UnitTests'
        component.database = 'sqlite://'
                
        drm = DummyRESTModel(component)
        
        # 2 positional args
        result = drm.list(*[123, 'abc'], **{})
        assert result == {'int':123, 'str':'abc'}
        # 2 query string args
        result = drm.list(*[], **{'int':123, 'str':'abc'})
        assert result == {'int':123, 'str':'abc'}
        
        # 1 positional, 1 keyword
        result = drm.list(*[123], **{'str':'abc'})
        assert result == {'int':123, 'str':'abc'}
        
    def testSanitiseAssertFail(self):
        
        config = Configuration()
        component = config.component_('UnitTests')
        component.application = 'UnitTests'
        component.database = 'sqlite://'
                
        drm = DummyRESTModel(component)
        
        # Wrong type for input args
        self.assertRaises(HTTPError, drm.list, *[123, 123], **{})
        self.assertRaises(HTTPError, drm.list, *['abc', 'abc'], **{})
        self.assertRaises(HTTPError, drm.list, *[], **{'str':123, 'int':'abc'})
        self.assertRaises(HTTPError, drm.list, *[], **{'str':'abc', 'int':'abc'})
        self.assertRaises(HTTPError, drm.list, *['abc', 123], **{})
        self.assertRaises(HTTPError, drm.list, *['abc', 'abc'], **{})
        self.assertRaises(HTTPError, drm.list, *[], **{'str':123, 'int':'abc'})
        self.assertRaises(HTTPError, drm.list, *[], **{'str':123, 'int':123})
        self.assertRaises(HTTPError, drm.list, *[], **{'str':'abc', 'int':'abc'})
        
        # Incorrect values for input args
        self.assertRaises(HTTPError, drm.list, *[1234, 'abc'], **{})
        self.assertRaises(HTTPError, drm.list, *[123, 'abcd'], **{})
        
    def testSanitiseKeyFail(self):
        config = Configuration()
        component = config.component_('UnitTests')
        component.application = 'UnitTests'
        component.database = 'sqlite://'
                
        drm = DummyRESTModel(component)
        
        # Empty input data, when data is required
        self.assertRaises(HTTPError, drm.list, [], {})
        # Out of order input data
        self.assertRaises(HTTPError, drm.list, ['abc'], {'int':123})
        
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
        
        
if __name__ == "__main__":
    unittest.main() 