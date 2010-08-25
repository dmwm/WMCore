#!/usr/bin/env python
"""
_Trigger_t_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: REST_t.py,v 1.2 2009/11/23 17:41:55 metson Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import os
import threading

import urllib, urllib2
from httplib import HTTPConnection

from WMQuality.TestInit import TestInit
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.Configuration import Configuration

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
        
    def list(self, args, kwargs):
        input = self.sanitise_input('list', args, kwargs)
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
    
    
    def testA(self):
        def makeRequest(uri='/rest/', values=None, type='GET'):
            headers = {}
            headers = {"Content-type": "application/x-www-form-urlencoded",
                       "Accept": "text/plain"}
            
            data = urllib.urlencode(values)
            if type != 'POST':
                uri = '%s?%s' % (uri, data)
            conn = HTTPConnection('localhost:8080')
            conn.connect()
            conn.request(type, uri, data, headers)
            response = conn.getresponse()
                
            data = response.read()
            conn.close()
            return data, response.status
            
        for t in ['GET', 'POST', 'PUT', 'DELETE', 'UPDATE']:
                response = makeRequest(values={'value':1234})
                assert response[1] == 200, \
                    "%s failed: %s - %s" % (t, response[1], response[2])
    
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
        self.assertRaises(AssertionError, drm.list, [123, 123], {})
        self.assertRaises(AssertionError, drm.list, ['abc', 'abc'], {})
        self.assertRaises(AssertionError, drm.list, [], {'str':123, 'int':'abc'})
        self.assertRaises(AssertionError, drm.list, [], {'str':'abc', 'int':'abc'})
        self.assertRaises(AssertionError, drm.list, ['abc', 123], {})
        self.assertRaises(AssertionError, drm.list, ['abc', 'abc'], {})
        self.assertRaises(AssertionError, drm.list, [], {'str':123, 'int':'abc'})
        self.assertRaises(AssertionError, drm.list, [], {'str':123, 'int':123})
        self.assertRaises(AssertionError, drm.list, [], {'str':'abc', 'int':'abc'})
        
        # Incorrect values for input args
        self.assertRaises(AssertionError, drm.list, [1234, 'abc'], {})
        self.assertRaises(AssertionError, drm.list, [123, 'abcd'], {})
        
    def testSanitiseKeyFail(self):
        config = Configuration()
        component = config.component_('UnitTests')
        component.application = 'UnitTests'
        component.database = 'sqlite://'
                
        drm = DummyRESTModel(component)
        
        # Empty input data, when data is required
        self.assertRaises(KeyError, drm.list, [], {})
        # Out of order input data
        self.assertRaises(KeyError, drm.list, ['abc'], {'int':123})
        
if __name__ == "__main__":
    unittest.main() 