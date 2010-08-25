#!/usr/bin/env python
"""
_Trigger_t_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: REST_t.py,v 1.4 2009/12/22 16:46:55 metson Exp $"
__version__ = "$Revision: 1.4 $"

import unittest
import os
import threading
import logging 
import cherrypy
import urllib, urllib2
from httplib import HTTPConnection
from cherrypy import HTTPError
from WMQuality.TestInit import TestInit
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.Configuration import Configuration
from WMCore.WebTools.Root import Root

class DummyDAO1:
    def execute(self, input={}):
        return {'data': 123, 'input': input}

class DummyDAO2:
    def execute(self, input={}):
        return {'data': 456, 'input': input}

class DummyDAOFac:
    def __call__(self, classname='DummyDAO1'):
        dao = None
        if classname == 'DummyDAO1':
            dao = DummyDAO1()
        elif classname == 'DummyDAO2':
            dao = DummyDAO2()
        return dao

class DummyRESTModel(RESTModel):
    def __init__(self, config):
        '''
        Initialise the RESTModel and add some methods to it.
        '''
        RESTModel.__init__(self, config)
        self.methods['GET'] = {'list':{'args':['int', 'str'],
                                        'call': self.list,
                                        'version': 2,
                                        'validation': [self.val_1, 
                                                       self.val_2, 
                                                       self.val_3, 
                                                       self.val_4]}}
        self.daofactory = DummyDAOFac()
        self.addDAO('GET', 'data1', 'DummyDAO1', ['num'])
        self.addDAO('GET', 'data2', 'DummyDAO2', ['num'])
        
    def list(self, args, kwargs):
        input = self.sanitise_input(args, kwargs, 'list')
        return input
    
    def val_1(self, input):
        # Convert the input data to an int (will be a string), ignore if it 
        # fails as the next validation will kill that, and it makes the unit test
        # trickier...
        try:
            input['int'] = int(input['int'])
        except:
            pass
        # Checks its first input contains a int
        assert type(input['int']) == type(123)
        return input
    
    def val_2(self, input):
        # Checks its second input is a string
        assert type(input['str']) == type('abc')
        return input
    
    def val_3(self, input):
        # Checks the int is 123
        assert input['int'] == 123
        return input
    
    def val_4(self, input):
        # Checks the str is 'abc'
        assert input['str'] == 'abc'
        return input
        
class RESTTest(unittest.TestCase):
    
    def configureServer(self):
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
        active.rest.model.object = 'WMCore.WebTools.RESTModel'
        active.rest.section_('formatter')
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
            data, code, type = self.makeRequest('/rest/echo', 
                                                   {'data': 'unit test'}, 
                                                   method, 'text/json')
            assert code == 200, \
                 'Got a return code != 200 (got %s)' % code
            assert type == 'text/json'
            assert data == '{"args": [], "kwargs": {"data": "unit test"}}', 'got unexpected response %s' % data
    
    def testGoodEchoWithPosArg(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        for method in ['POST']:
            data, code, type = self.makeRequest('/rest/echo/stuff', 
                                                {'data': 'unit test'},
                                                method, 
                                                'text/json')
            assert code == 200, \
                 'Got a return code != 200 (got %s)' % code
            assert type == 'text/json'
            assert data == '{"args": ["stuff"], "kwargs": {"data": "unit test"}}', 'got unexpected response %s' % data
            
    def testBadMethodEcho(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        
        for method in ['GET']:
            data, code, type = self.makeRequest('/rest/echo', {'data': 'unit test'}, 
                                          method, 'text/json')
            assert int(code) == 405, "Didn't get a 'Method Not Allowed' response for %s (got %s)" % (method, code)
            assert type == 'text/json' 
        rt.stop()
          
    def testBadVerbEcho(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        
        for method in ['DELETE', 'PUT']:
            data, code, type = self.makeRequest('/rest/echo', {'data': 'unit test'}, 
                                          method, 'text/json')
            assert int(code) == 501, "Didn't get a 'Not Implemented' response for %s (got %s), message: %s" % (method, code, data)
            assert type == 'text/json'
        rt.stop()
    
    def testPing(self):
        rt = self.configureServer()
        rt.start(blocking=False)
        cherrypy.log.error_log.setLevel(logging.WARNING)
        cherrypy.log.access_log.setLevel(logging.WARNING)
        data, code, type = self.makeRequest('/rest/ping', 
                                          type='GET', accept='text/json')
        assert code == 200, 'Got a return code != 200 (got %s), message: %s' % (code, data)
        assert type == 'text/json'
        assert data == '"ping"', 'got unexpected response %s' % data
        rt.stop()
    
    def makeRequest(self, uri='/rest/', values=None, type='GET', accept="text/plain"):
        headers = {}
        headers = {"Content-type": "application/x-www-form-urlencoded",
                   "Accept": accept}
        data = None
        if values:
            data = urllib.urlencode(values)
        if type != 'POST':
            uri = '%s?%s' % (uri, data)
        conn = HTTPConnection('localhost:8080')
        conn.connect()
        conn.request(type, uri, data, headers)
        response = conn.getresponse()
        
        data = response.read()
        conn.close()
        return data, response.status, response.getheader('content-type').split(';')[0]
    
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
        result = drm.list([123, 'abc'], {})
        assert result == {'int':123, 'str':'abc'}
        # 2 query string args
        result = drm.list([], {'int':123, 'str':'abc'})
        assert result == {'int':123, 'str':'abc'}
        
        # 1 positional, 1 keyword
        result = drm.list([123], {'str':'abc'})
        assert result == {'int':123, 'str':'abc'}
        
    def testSanitiseAssertFail(self):
        
        config = Configuration()
        component = config.component_('UnitTests')
        component.application = 'UnitTests'
        component.database = 'sqlite://'
                
        drm = DummyRESTModel(component)
        
        # Wrong type for input args
        self.assertRaises(HTTPError, drm.list, [123, 123], {})
        self.assertRaises(HTTPError, drm.list, ['abc', 'abc'], {})
        self.assertRaises(HTTPError, drm.list, [], {'str':123, 'int':'abc'})
        self.assertRaises(HTTPError, drm.list, [], {'str':'abc', 'int':'abc'})
        self.assertRaises(HTTPError, drm.list, ['abc', 123], {})
        self.assertRaises(HTTPError, drm.list, ['abc', 'abc'], {})
        self.assertRaises(HTTPError, drm.list, [], {'str':123, 'int':'abc'})
        self.assertRaises(HTTPError, drm.list, [], {'str':123, 'int':123})
        self.assertRaises(HTTPError, drm.list, [], {'str':'abc', 'int':'abc'})
        
        # Incorrect values for input args
        self.assertRaises(HTTPError, drm.list, [1234, 'abc'], {})
        self.assertRaises(HTTPError, drm.list, [123, 'abcd'], {})
        
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
        
        result = drm.methods['GET']['data1']['call']([], {})
        assert result['data'] == 123
        assert result['input'] == {}
        
        result =  drm.methods['GET']['data1']['call']([123], {})
        assert result['data'] == 123
        assert result['input'] == {'num': 123}
        
        result =  drm.methods['GET']['data2']['call']([], {})
        assert result['data'] == 456
        assert result['input'] == {}
        
        result =  drm.methods['GET']['data2']['call']([456], {})
        assert result['data'] == 456
        assert result['input'] == {'num': 456}
        
        
if __name__ == "__main__":
    unittest.main() 