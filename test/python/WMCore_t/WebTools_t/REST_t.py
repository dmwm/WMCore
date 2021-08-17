#!/usr/bin/env python
"""
_REST_t_

Unit tests for checking RESTModel works correctly

TODO: duplicate all direct call tests to ones that use HTTP
"""

from future import standard_library
standard_library.install_aliases()

import unittest
import logging
import urllib.request, urllib.error
import json

from cherrypy import HTTPError
from nose.plugins.attrib import attr
from tempfile import NamedTemporaryFile

from WMCore_t.WebTools_t.DummyRESTModel import DummyRESTModel
from WMCore_t.WebTools_t.DummyRESTModel import (DUMMY_ROLE, DUMMY_GROUP, DUMMY_SITE)

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import (DefaultConfig, cherrypySetup)
from WMQuality.WebTools.RESTClientAPI import (makeRequest, methodTest)


secureConfig = DefaultConfig('WMCore_t.WebTools_t.DummyRESTModel')
secureConfig.SecurityModule.dangerously_insecure = False
secureConfig.Webtools.environment = 'production'
tempFile = NamedTemporaryFile()
secureConfig.SecurityModule.key_file = tempFile.name
f = open(tempFile.name,"rb")
secureKey = f.read()
secureConfig.SecurityModule.section_("default")
secureConfig.SecurityModule.default.role = DUMMY_ROLE
secureConfig.SecurityModule.default.group = DUMMY_GROUP
secureConfig.SecurityModule.default.site = DUMMY_SITE

class RESTTest(RESTBaseUnitTest):
    def initialize(self):
        self.config = DefaultConfig('WMCore_t.WebTools_t.DummyRESTModel')
        self.do_debug = False
        self.do_production = False

        if self.do_production:
            self.config.Webtools.environment = 'production'
            self.config.SecurityModule.dangerously_insecure = False
            # not real keyfile but for the test.
            # file will be deleted automaticall when garbage collected.
            self.tempFile = NamedTemporaryFile()
            self.config.SecurityModule.key_file = self.tempFile.name
            self.config.SecurityModule.section_("default")
            self.config.SecurityModule.default.role = ""
            self.config.SecurityModule.default.group = ""
            self.config.SecurityModule.default.site = ""

        if not self.do_debug:
            self.config.Webtools.error_log_level = logging.WARNING
            self.config.Webtools.access_log_level = logging.WARNING

        self.urlbase = self.config.getServerUrl()

    def testGeneratorMethod(self):
        # test not accepted type should return 406 error
        url = self.urlbase + 'gen'
        output={'code':200}
        data, _ = methodTest('GET', url, accept='text/json', output=output)
        data = json.loads(data)
        self.assertEqual(type(data), list)
        self.assertEqual(type(data[0]), dict)

    def testUnsupportedFormat(self):
        # test not accepted type should return 406 error
        url = self.urlbase + 'ping'
        methodTest('GET', url, accept='text/das', output={'code':406})

    def testGoodEcho(self):
        verb ='POST'
        url = self.urlbase + 'echo'
        input_data={'message': 'unit test'}
        output={'code':200, 'type':'text/json',
              'data':'{"message": "unit test"}'}

        methodTest(verb, url, input_data, output=output)

    def testBadEchoWithPosArg(self):
        "Echo takes one argument (message), with the positional argument it should fail"
        verb ='POST'
        url = self.urlbase + 'echo/stuff'
        input_data={'message': 'unit test'}
        output={'code':400, 'type':'text/json'}
        methodTest(verb, url, input_data, output=output)

    def testBadMethodEcho(self):
        """
        The echo method isn't supported by GET, so should raise a 405
        """
        verb ='GET'
        url = self.urlbase + 'echo'
        input_data = {'data': 'unit test'}
        output = {'code':405, 'type':'text/json'}

        methodTest(verb, url, input_data, output=output)

    def testBadVerbEcho(self):
        "echo is only available to GET and POST, so should raise a 501"
        url = self.urlbase + 'echo'
        input_data = {'data': 'unit test'}
        output = {'code':501, 'type':'text/json'}

        for verb in ['DELETE']:
            methodTest(verb, url, input_data, output=output)

    def testPing(self):
        verb ='GET'
        url = self.urlbase + 'ping'
        output={'code':200, 'type':'text/json', 'data':'"ping"'}
        expireTime =3600

        methodTest(verb, url, output=output, expireTime=expireTime)

    def testBadPing(self):
        verb ='GET'

        url = self.urlbase + 'wrong'
        output={'code':404}
        methodTest(verb, url, output=output)

        url = self.urlbase + 'echo'
        output={'code':405}
        methodTest(verb, url, output=output)


        url = self.urlbase + 'ping/wrong'
        output={'code':400}
        methodTest(verb, url, output=output)

    def testException(self):
        """
        testException

        list takes a single integer argument, querying with a string
        """
        url = self.urlbase + 'list?int=a'
        self.assertRaises(urllib.error.HTTPError, urllib.request.urlopen, url)
        try:
            urllib.request.urlopen(url)
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)
            self.assertEqual(e.reason, u'Bad Request')
            self.assertEqual(e.msg, u'Bad Request')
            exception_data = json.loads(e.read())
            self.assertEqual(exception_data['type'], 'HTTPError')
            self.assertEqual(exception_data['message'], 'Invalid input: Input arguments failed sanitation.')

        url = self.urlbase + 'list1?int=a'
        self.assertRaises(urllib.error.HTTPError, urllib.request.urlopen, url)
        try:
            urllib.request.urlopen(url)
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 400)
            self.assertEqual(e.reason, u'Bad Request')
            self.assertEqual(e.msg, u'Bad Request')
            exception_data = json.loads(e.read())
            self.assertEqual(exception_data['type'], 'HTTPError')
            self.assertEqual(exception_data['message'], 'Invalid input: Arguments added where none allowed')

    def testList(self):
        verb ='GET'
        url = self.urlbase + 'list/'
        request_input = {'input_int':123, 'input_str':'abc'}
        output={'code':200, 'type':'text/json'}
        result = json.loads(methodTest(verb, url, request_input=request_input, output=output)[0])
        for i in result.keys():
            self.assertEqual(result[i], request_input[i], '%s does not match response' % i)

    def testA(self):
        # This test doesn't actually use the type, just the same thing 5 times.
        for t in ['GET', 'POST', 'PUT', 'DELETE', 'UPDATE']:
            response = makeRequest(url=self.urlbase + '/', values={'value':1234})
            assert response[1] == 200, 'Got a return code != 200 (got %s)' % response[1]

    def testSanitisePass(self):
        """
        Emulate how CherryPy passes arguments to a method, check that the data
        returned is correct.

        No server setup required
        """
        drm = DummyRESTModel(self.config.getModelConfig())

        def func(*args, **kwargs):
            sanitised_input = drm._sanitise_input(args, kwargs, "list")
            return drm.list(**sanitised_input)

        # 2 positional args (e.g. url/arg1/arg2)
        result = func(123, 'abc')
        assert result == {'input_int':123, 'input_str':'abc'},\
                                'list with 2 positional args failed: %s' % result
        # 2 query string args (e.g. url?int=arg1&str=arg2)
        result = func(input_int=123, input_str='abc')
        assert result == {'input_int':123, 'input_str':'abc'},\
                               'list with 2 query string args failed: %s' % result

        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        result = func(123, input_str='abc')
        assert result == {'input_int':123, 'input_str':'abc'},\
               'list with 1 positional, 1 keyword failed: %s' % result

    def testSanitisePassHTTP(self):
        """
        Same as testSanitisePass but do it over http and check the returned http
        codes.
        """
        # 2 positional args (e.g. url/arg1/arg2)
        url = self.urlbase + 'list/123/abc'
        response = makeRequest(url=url)
        assert response[1] == 200, \
             'list with 2 positional args failed: ' +\
             '. Got a return code != 200 (got %s)' % response[1] +\
             '. Returned data: %s' % response[0]
              # 2 query string args (e.g. url?int=arg1&str=arg2)
        url = self.urlbase + 'list/'
        response = makeRequest(url=url, values={'input_int':'123', 'input_str':'abc'})
        assert response[1] == 200, \
                 'list with 2 query string args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]

        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        url = self.urlbase + 'list/123/'
        response = makeRequest(url=url,
                                     values={'input_str':'abc'})
        assert response[1] == 200, \
                 'list with 1 positional, 1 keyword failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]

    def testSanitiseAssertFail(self):
        """
        No server set up required, the purpose of the test is just
        demonstrating how validation is used.
        """
        drm = DummyRESTModel(self.config.getModelConfig())

        def func(*args, **kwargs):
            sanitised_input = drm._sanitise_input(args, kwargs, "list")
            return drm.list(**sanitised_input)

        # Wrong type for input args
        self.assertRaises(HTTPError, func, 123, 123)
        self.assertRaises(HTTPError, func, 'abc', 'abc')
        self.assertRaises(HTTPError, func, input_str = 'abc', input_int = 123, other='dfe')
        self.assertRaises(HTTPError, func, str = 123, int ='abc')
        self.assertRaises(HTTPError, func, str =' abc', int = 'abc')
        self.assertRaises(HTTPError, func, 'abc', 123)
        self.assertRaises(HTTPError, func, 'abc', 'abc')
        self.assertRaises(HTTPError, func, str = 123, int = 'abc')
        self.assertRaises(HTTPError, func, str =123, int = 123)
        self.assertRaises(HTTPError, func, str = 'abc', int ='abc')

        # Incorrect values for input args
        self.assertRaises(HTTPError, func, 1234, 'abc')
        self.assertRaises(HTTPError, func, 123, 'abcd')

        # Empty input data, when data is required
        self.assertRaises(HTTPError, func)

    def testSanitiseFailHTTP(self):
        """
        Same as testSanitisePass but do it over http and check the returned http
        codes.
        """
        # 2 positional args (e.g. url/arg1/arg2)
        url = self.urlbase + 'list/123/'
        response = makeRequest(url=url, accept='text/json')
        assert response[1] == 400, \
                 'list with 2 positional args failed: ' +\
                 '. Got a return code != 400 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]

        self.assertEqual(response[2], 'text/json', 'type is not text/json : %s' % type)
        # 2 query string args (e.g. url?int=arg1&str=arg2)
        url = self.urlbase + 'list'
        response = makeRequest(url=url, values={'int':'abc', 'str':'abc'})
        assert response[1] == 400, \
                 'list with 2 query string args failed: ' +\
                 '. Got a return code != 400 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]

        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        url = self.urlbase + 'list/abc'
        response = makeRequest(url=url, values={'str':'abc'})
        assert response[1] == 400, \
                 'list with 1 positional, 1 keyword failed: ' +\
                 '. Got a return code != 400 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]

    # don't need server set up
    def testDAOBased(self):
        drm = DummyRESTModel(self.config.getModelConfig())

        result = drm.methods['GET']['data1']['call']()
        self.assertEqual( result ,  123, 'Error default value is set to 123 but returns %s' % result )

        result =  drm.methods['GET']['data2']['call'](456)
        self.assertEqual( result['num'] ,  456 )

        result =  drm.methods['GET']['data2']['call'](num = 456)
        self.assertEqual( result['num'] ,  456 )

        result =  drm.methods['GET']['data3']['call'](num = 456, thing="TEST")
        self.assertEqual( result['num'] == 456 and result['thing'] ,  "TEST" )

    # This test is flipping back and forth in Jenkins. Perhaps due to port 8888 not being available.
    # Disabling for now
    @attr("integration")
    def testDAOBasedHTTP(self):
        """
        Same as testSanitisePass but do it over http and check the returned http
        codes.
        """
        # 2 positional args (e.g. url/arg1/arg2)
        url = self.urlbase + 'data1/'
        response = makeRequest(url=url)
        assert response[1] == 200, \
                 'dao without args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]
        self.assertEqual( response[0] ,  '123', response[0])

        # 2 query string args (e.g. url?int=arg1&str=arg2)
        url = self.urlbase + 'data2'
        response = makeRequest(url=url, values={'num':456})

        assert response[1] == 200, \
                 'dao with 1 args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]
        #Warning quotation type matters
        #Should use encoded and decoded format
        self.assertEqual( response[0] ,  "{'num': '456'}", "should be {'num': '456'} but got %s" % response[0]          )

        # 1 positional, 1 keyword  (e.g. url/arg1/?str=arg2)
        url = self.urlbase + 'data3/123'
        response = makeRequest(url=url, values={'thing':'abc'})

        assert response[1] == 200, \
                 'dao with 1 positional, 1 keyword failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]
        #Warning quotation type and order matters
        #Should use encoded and decoded format
        self.assertEqual( response[0] ,  "{'thing': 'abc', 'num': '123'}", "should be {'thing': 'abc', 'num': '123'} but got %s" % response[0] )

    def testListTypeArgs(self):
        # 2 positional args (e.g. url/arg1/arg2)
        url = self.urlbase + 'listTypeArgs?aList=1'
        response = makeRequest(url=url)
        assert response[1] == 200 and response[0] == b"[1]", \
                 'list args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]


        # 2 values with the same keywords (e.g. url/arg1/arg2)
        url = self.urlbase + 'listTypeArgs?aList=1&aList=2'
        response = makeRequest(url=url)
        assert response[1] == 200 and response[0] == b"[1, 2]", \
                 'list args failed: ' +\
                 '. Got a return code != 200 (got %s)' % response[1] +\
                 '. Returned data: %s' % response[0]

    @cherrypySetup(secureConfig)
    @attr("integration")
    def testAuthentication(self):
        verb ='PUT'
        url = self.urlbase + 'list1'
        urllib_data = urllib.request.urlopen(url)
        self.assertEqual(urllib_data.getcode(), 403)

        # pass proper role
        output={'code':200}
        methodTest(verb, url, output=output,
                   secure=True, secureParam={'key': secureKey,
                                             'role': DUMMY_ROLE,
                                             'group': DUMMY_GROUP,
                                             'site': DUMMY_SITE})

if __name__ == "__main__":
    unittest.main()
