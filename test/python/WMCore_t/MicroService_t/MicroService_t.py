"""
Unit tests for MicroService.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""

from __future__ import division, print_function

import unittest

import cherrypy
from WMCore_t.MicroService_t import TestConfig
from WMCore.MicroService.Service.RestApiHub import RestApiHub
from WMCore.MicroService.Tools.Common import cert, ckey
from WMCore.Services.pycurl_manager import RequestHandler


class ServiceManager(object):
    """
    Initialize ServiceManager class
    """

    def __init__(self, config=None):
        self.config = config
        self.appname = 'test'  # keep it since it is used by XMLFormat(self.app.appname))

    def status(self, serviceName=None, **kwargs):
        "Return current status about our service"
        print("### CALL status API with service name %s" % serviceName)
        data = {'status': "OK", "api": "status"}
        if kwargs:
            data.update(kwargs)
        return data

    def info(self, reqName, **kwargs):
        "Return current status about our service"
        print("### CALL info API with request name %s" % reqName)
        data = {'status': "OK", "api": "info"}
        if kwargs:
            data.update(kwargs)
        return data


class MicroServiceTest(unittest.TestCase):
    "Unit test for MicroService module"

    def setUp(self):
        "Setup MicroService for testing"
        self.managerName = "ServiceManager"
        config = TestConfig
        manager = 'WMCore_t.MicroService_t.MicroService_t.%s' % self.managerName
        config.views.data.manager = manager
        config.manager = manager
        mount = '/microservice/data'
        self.mgr = RequestHandler()
        self.port = config.main.port
        self.url = 'http://localhost:%s%s' % (self.port, mount)
        cherrypy.config["server.socket_port"] = self.port
        self.app = ServiceManager(config)
        self.server = RestApiHub(self.app, config, mount)
        cherrypy.tree.mount(self.server, mount)
        cherrypy.engine.start()

    def tearDown(self):
        "Tear down MicroService"
        cherrypy.engine.stop()
        cherrypy.engine.exit()

    def postRequest(self, apiName, params):
        "Perform POST request to our MicroService"
        headers = {'Content-type': 'application/json'}
        url = self.url + "/%s" % apiName
        data = self.mgr.getdata(url, params=params, headers=headers, \
                                verb='POST', cert=cert(), ckey=ckey(), encode=True, decode=True)
        print("### post call data %s" % data)
        return data

    def testGetStatus(self):
        "Test function for getting state of the MicroService"
        api = "status"
        url = '%s/%s' % (self.url, api)
        params = {}
        data = self.mgr.getdata(url, params=params, encode=True, decode=True)
        self.assertEqual(data['result'][0]['microservice'], self.managerName)
        self.assertEqual(data['result'][0]['api'], api)

        params = {"service": "transferor"}
        data = self.mgr.getdata(url, params=params, encode=True, decode=True)
        self.assertEqual(data['result'][0]['microservice'], self.managerName)
        self.assertEqual(data['result'][0]['api'], api)

    def testGetInfo(self):
        "Test function for getting state of the MicroService"
        api = "status"
        url = '%s/%s' % (self.url, api)
        params = {}
        data = self.mgr.getdata(url, params=params, encode=True, decode=True)
        self.assertEqual(data['result'][0]['microservice'], self.managerName)
        self.assertEqual(data['result'][0]['api'], api)

        params = {"request": "fake_request_name"}
        data = self.mgr.getdata(url, params=params, encode=True, decode=True)
        self.assertEqual(data['result'][0]['microservice'], self.managerName)
        self.assertEqual(data['result'][0]['api'], api)

    def testPostCall(self):
        "Test function for getting state of the MicroService"
        api = "status"
        data = {"request": "fake_request_name"}
        data = self.postRequest(api, data)
        self.assertDictEqual(data['result'][0], {'status': 'OK', 'api': 'info'})


if __name__ == '__main__':
    unittest.main()
