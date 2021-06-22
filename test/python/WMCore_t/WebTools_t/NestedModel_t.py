import logging
import unittest

from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTClientAPI import methodTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig


class NestedModelTest(RESTBaseUnitTest):

    def initialize(self):
        self.config = DefaultConfig('WMCore_t.WebTools_t.DummyNestedModel')
        do_debug = True

        self.config.Webtools.environment = 'development'
        if do_debug:
            self.config.Webtools.error_log_level = logging.DEBUG
            self.config.Webtools.access_log_level = logging.DEBUG
        else:
            self.config.Webtools.error_log_level = logging.WARNING
            self.config.Webtools.access_log_level = logging.WARNING

        self.urlbase = self.config.getServerUrl()

    def testOuterFooPass(self):
        verb = 'GET'
        url = self.urlbase + 'foo'
        output = {'code': 200, 'data': b'"foo"'}
        expireTime = 3600
        methodTest(verb, url, output=output, expireTime=expireTime)

        url = self.urlbase + 'foo/test'
        output = {'code': 200, 'data': b'"foo test"'}
        methodTest(verb, url, output=output, expireTime=expireTime)

        url = self.urlbase + 'foo'
        request_input = {'message': "test"}
        output = {'code': 200, 'data': b'"foo test"'}
        methodTest(verb, url, request_input=request_input, output=output, expireTime=expireTime)

    def testInnerPingPass(self):
        verb = 'GET'
        url = self.urlbase + 'foo/ping'
        output = {'code': 200, 'data': b'"ping"'}
        expireTime = 3600

        methodTest(verb, url, output=output, expireTime=expireTime)

    def testOuterFooError(self):
        verb = 'GET'
        url = self.urlbase + 'foo/123/567'
        output = {'code': 400}
        methodTest(verb, url, output=output)

    def testInnerPingError(self):
        verb = 'GET'
        url = self.urlbase + 'foo/123/ping'
        output = {'code': 400}
        methodTest(verb, url, output=output)

        url = self.urlbase + 'foo/ping/123'
        methodTest(verb, url, output=output)


if __name__ == "__main__":
    unittest.main()
