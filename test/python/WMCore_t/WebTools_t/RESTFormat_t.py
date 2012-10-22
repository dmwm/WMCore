#!/usr/bin/env python
"""
_REST_t_

Unit tests for checking RESTModel works correctly

TODO: duplicate all direct call tests to ones that use HTTP
"""

import unittest
import logging
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
        do_debug = False

        self.config.Webtools.environment = 'development'
        if do_debug:
            self.config.Webtools.error_log_level = logging.DEBUG
            self.config.Webtools.access_log_level = logging.DEBUG
        else:
            self.config.Webtools.error_log_level = logging.WARNING
            self.config.Webtools.access_log_level = logging.WARNING

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
        methodTest('GET', url, accept=type,
                         output={'code':200, 'data':"{'a': 'a%', 'b': 'b'}"})

        request_input={'a':'%', 'b':'b'}

        #methodTest encoded input with urlencode
        url = self.urlbase +'list3'
        methodTest('GET', url, accept=type, request_input=request_input,
                 output={'code':200, 'data':"{'a': '%', 'b': 'b'}"})

    def testReturnFormat(self):
        return_type = 'application/json'

        url = self.urlbase +'list3?a=a%&b=b'
        methodTest('GET', url, accept=return_type,
                         output={'code':200, 'data':'{"a": "a%", "b": "b"}'})

        url = self.urlbase + 'list?input_int=a&input_str=a'
        expected_data = '''{"exception": 400, "message": "Invalid input: Input data failed validation.", "type": "HTTPError"}'''
        methodTest('GET', url, accept=return_type,
                         output={'code':400, 'data':expected_data})

    def testNoArgMethods(self):
        """
        list1 takes no arguments, it should raise an error if called with one. Require json output.
        """
        return_type = 'application/json'
        url = self.urlbase + 'list1?int=a'
        expected_data = """{"exception": 400, "message": "Invalid input: Arguments added where none allowed", "type": "HTTPError"}"""
        methodTest('GET', url, accept=return_type, output={'code':400, 'data':expected_data})


if __name__ == "__main__":
    unittest.main()
