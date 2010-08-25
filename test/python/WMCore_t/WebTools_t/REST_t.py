#!/usr/bin/env python
"""
_Trigger_t_

Unit tests for message services: subscription, priority subscription, buffers,
etc..

"""

__revision__ = "$Id: REST_t.py,v 1.1 2009/04/28 04:49:18 metson Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import os
import threading

import urllib, urllib2
from httplib import HTTPConnection

from WMQuality.TestInit import TestInit

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
                
            #print type, uri, response.status, response.reason  #TODO: log this not print
            data = response.read()
            conn.close()
            return data, response.status
            
        for t in ['GET', 'POST', 'PUT', 'DELETE', 'UPDATE']:
                response = makeRequest(values={'value':1234})
                assert response[1] == 200, \
                    "%s failed: %s - %s" % (t, response[1], response[2])
        