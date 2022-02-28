#!/usr/bin/env python
"""
_REST_t_

Unit tests for checking RESTModel works correctly

TODO: duplicate all direct call tests to ones that use HTTP
"""

import json
import unittest
import logging

from nose.plugins.attrib import attr

from WMCore.WebTools.RESTFormatter import RESTFormatter
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTClientAPI import methodTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig

from Utils.PythonVersion import PY3

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

        for textType in rf.supporttypes.keys():
            # test accepted type should return 200 error
            methodTest('GET', url, accept=textType, output={'code':200})

    # This test is flipping back and forth in Jenkins. Perhaps due to port 8888 not being available.
    # Disabling for now
    @attr("integration")
    def testEncodedInput(self):
        textType = 'text/plain'

        url = self.urlbase + 'list3?a=a%&b=b'
        data = json.dumps({'a': 'a%', 'b': 'b'})
        methodTest('GET', url, accept=textType,
                         output={'code':200, 'data':data})

        request_input={'a':'%', 'b':'b'}

        #methodTest encoded input with urlencode
        url = self.urlbase +'list3'
        data = json.dumps({'a': '%', 'b': 'b'})
        methodTest('GET', url, accept=textType, request_input=request_input,
                 output={'code':200, 'data':data})

    def testReturnFormat(self):
        return_type = 'application/json'

        url = self.urlbase +'list3?a=a%&b=b'
        methodTest('GET', url, accept=return_type,
                         output={'code':200, 'data':'{"a": "a%", "b": "b"}'})

        url = self.urlbase + 'list?input_int=a&input_str=a'
        expected_data_py2 = '{"exception": 400, "message": "Invalid input: Input data failed validation.", "type": "HTTPError"}'
        expected_data_py3 = '{"exception": 400, "type": "HTTPError", "message": "Invalid input: Input data failed validation."}'
        expected_data = expected_data_py3 if PY3 else expected_data_py2
        methodTest('GET', url, accept=return_type,
                         output={'code':400, 'data':expected_data})

    def testNoArgMethods(self):
        """
        list1 takes no arguments, it should raise an error if called with one. Require json output.
        """
        return_type = 'application/json'
        url = self.urlbase + 'list1?int=a'
        expected_data_py2 = '{"exception": 400, "message": "Invalid input: Arguments added where none allowed", "type": "HTTPError"}'
        expected_data_py3 = '{"exception": 400, "type": "HTTPError", "message": "Invalid input: Arguments added where none allowed"}'
        expected_data = expected_data_py3 if PY3 else expected_data_py2
        methodTest('GET', url, accept=return_type, output={'code':400, 'data':expected_data})

    def testGenerator(self):
        rf = RESTFormatter(config=self.config.Webtools)
        url = self.urlbase +'gen'
        # gen method from DummyRESTModel will return this generator
        gen = ({'idx':i} for i in range(10))
        # the WMCore should convert it into list regardless of accept type
        data = rf.json(gen)
        methodTest('GET', url, accept='application/json',
                         output={'code':200, 'data':data})
        methodTest('GET', url, accept='*/*',
                         output={'code':200, 'data':data})

if __name__ == "__main__":
    unittest.main()
