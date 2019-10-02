"""
Unit tests for MicroService.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

from __future__ import division, print_function

import json
import unittest

import cherrypy
from WMCore_t.MicroService_t import TestConfig

from WMCore.MicroService.Service.RestApiHub import RestApiHub
from WMCore.MicroService.Unified.Common import cert, ckey
from WMCore.Services.pycurl_manager import RequestHandler


class ServiceManager(object):
    """
    Initialize ServiceManager class
    """

    def __init__(self, config=None):
        self.config = config
        self.state = None
        self.appname = 'test'  # keep it since it is used by XMLFormat(self.app.appname))

    def status(self, **kwargs):
        "Return current status about our service"
        print("### CALL status %s" % kwargs)
        return {'state': self.state}

    def request(self, **kwargs):
        "Process request given to us"
        print("### CALL request %s" % kwargs)
        self.state = kwargs.get('state', None)
        return {'state': self.state}


class MicroServiceTest(unittest.TestCase):
    "Unit test for MicroService module"

    def setUp(self):
        "Setup MicroService for testing"
        self.app = ServiceManager()
        config = TestConfig
        manager = 'WMCore_t.Services_t.MicroService_t.MicroService_t.ServiceManager'
        config.views.data.manager = manager
        config.manager = manager
        mount = '/microservice'
        self.mgr = RequestHandler()
        self.port = config.main.port
        self.url = 'http://localhost:%s%s/data' % (self.port, mount)
        cherrypy.config["server.socket_port"] = self.port
        self.server = RestApiHub(self.app, config, mount)
        cherrypy.tree.mount(self.server, mount)
        cherrypy.engine.start()

    def tearDown(self):
        "Tear down MicroService"
        cherrypy.engine.exit()
        cherrypy.engine.stop()

    def postRequest(self, params):
        "Perform POST request to our MicroService"
        headers = {'Content-type': 'application/json'}
        print("### post call %s params=%s headers=%s" % (self.url, params, headers))
        data = self.mgr.getdata(self.url, params=params, headers=headers, \
                                verb='POST', cert=cert(), ckey=ckey(), encode=True, decode=True)
        print("### post call data %s" % data)
        return data

    def test_getState(self):
        "Test function for getting state of the MicroService"
        url = '%s/status' % self.url
        data = self.mgr.getdata(url, params={})
        state = "bla"
        data = {"request": {"state": state}}
        self.postRequest(data)
        data = self.mgr.getdata(url, params={})
        data = json.loads(data)
        print("### url=%s, data=%s" % (url, data))
        for row in data['result']:
            if 'state' in row:
                self.assertEqual(state, row['state'])


if __name__ == '__main__':
    unittest.main()
