#!/usr/bin/env python

from builtins import object
from future import standard_library
standard_library.install_aliases()

import unittest
import logging
import urllib.request
from WMCore.WebTools.Root import Root
from WMCore.Configuration import Configuration
from cherrypy import engine, tree
from cherrypy import config as cpconfig
from tempfile import NamedTemporaryFile

# DISABLING because this doesn't properly shut down the cherrypy
# server or clean up the state
class RootTest(object):

    def getBaseConfiguration(self):
        config = Configuration()
        config.component_('SecurityModule')
        config.SecurityModule.dangerously_insecure = True

        config.component_('Webtools')
        config.Webtools.application = 'UnitTests'
        config.Webtools.log_screen = False
        config.Webtools.environment = "development"
        config.Webtools.error_log_level = logging.WARNING
        config.Webtools.access_log_level = logging.DEBUG
        config.Webtools.host = "127.0.0.1"
        config.component_('UnitTests')
        config.UnitTests.admin = "Mr Unit Test"
        config.UnitTests.description = "Dummy server for unit tests"
        config.UnitTests.title = "Unit Tests"
        config.UnitTests.section_('views')
        active = config.UnitTests.views.section_('active')

        return config

    def testLongHandConfigurables(self):
        """
        Test that the following configuration variables work:

        engine      Controls the "application engine", including autoreload.
                    These can only be declared in the global config.
        hooks       Declares additional request-processing functions.
        log         Configures the logging for each application. These can only
                    be declared in the global or / config.
        request         Adds attributes to each Request.
        response        Adds attributes to each Response.
        server      Controls the default HTTP server via cherrypy.server. These
                    can only be declared in the global config.
        tools       Runs and configures additional request-processing packages.
        wsgi        Adds WSGI middleware to an Application's "pipeline". These
                    can only be declared in the app's root config ("/").
        checker         Controls the "checker", which looks for common errors in app
                    state (including config) when the engine starts. Global config only.

        (from http://docs.cherrypy.org/dev/intro/concepts/config.html)
        """
        config = self.getBaseConfiguration()

        server = Root(config)
        server.start(blocking=False)
        server.stop()

    def testFakeLongHandConfigurables(self):
        """
        Test that a made up long hand configurable is ignored
        """
        config = self.getBaseConfiguration()
        # The following should be ignored by the configure step
        config.Webtools.section_('foo')
        config.Webtools.foo.bar = 'baz'
        config.Webtools.section_('stuff')
        config.Webtools.stuff = 'things'

        server = Root(config)
        server.start(blocking=False)

        self.assertFalse('foo' in cpconfig, 'non-standard configurable passed to server')
        self.assertFalse('stuff' in cpconfig, 'non-standard configurable passed to server')

        server.stop()

    def testMissingRequiredConfigParams(self):
        """
        All applications should define:
        ['admin', 'description', 'title']
        """
        config = self.getBaseConfiguration()

        config.UnitTests.__delattr__('admin')
        server = Root(config)
        self.assertRaises(AssertionError, server.start, blocking=False)

        config.UnitTests.admin = "Mr Unit Test"
        config.UnitTests.__delattr__('description')
        server = Root(config)
        self.assertRaises(AssertionError, server.start, blocking=False)

        config.UnitTests.description = "Dummy server for unit tests"
        config.UnitTests.__delattr__('title')
        server = Root(config)
        self.assertRaises(AssertionError, server.start, blocking=False)

    def testLongHandProxyBase(self):
        """
        Check that changing the proxy base via tools.proxy.base
        does actually change the proxy base
        """
        test_proxy_base = '/unit_test'

        config = self.getBaseConfiguration()
        config.Webtools.section_('tools')
        config.Webtools.tools.section_('proxy')
        config.Webtools.tools.proxy.base = test_proxy_base
        config.Webtools.tools.proxy.on = True
        server = Root(config)

        server.start(blocking=False)
        self.assertEqual(cpconfig['tools.proxy.base'], test_proxy_base)
        server.stop()

    def testShortHandProxyBase(self):
        """
        Check that changing the proxy_base via the short hand config variable
        does actually change the proxy base
        """
        test_proxy_base = '/unit_test'

        config = self.getBaseConfiguration()
        # Set the proxy base with a short hand cfg variable
        config.Webtools.proxy_base = test_proxy_base
        server = Root(config)

        server.start(blocking=False)
        self.assertEqual(cpconfig['tools.proxy.base'], test_proxy_base)
        server.stop()

    def testLongHandChangePort(self):
        """
        Change the port the server runs on long hand
        """
        test_port = 8010

        config = self.getBaseConfiguration()
        # Set the port to a non-standard one
        config.Webtools.section_('server')
        config.Webtools.server.socket_port = test_port

        server = Root(config)
        server.start(blocking=False)
        self.assertEqual(cpconfig['server.socket_port'], test_port)
        server.stop()

    def testShortHandChangePort(self):
        """
        Change the port the server runs on short hand
        """
        test_port = 8010

        config = self.getBaseConfiguration()
        # Set the port to a non-standard one
        config.Webtools.port = test_port

        server = Root(config)
        server.start(blocking=False)
        self.assertEqual(cpconfig['server.socket_port'], test_port)
        server.stop()

    def testShortHandPortOverride(self):
        """
        Change the port the server runs on long hand, then over ride
        it with the short hand equivalent
        """
        test_port = 8010

        config = self.getBaseConfiguration()
        # Set the port the long handed way
        config.Webtools.section_('server')
        config.Webtools.server.socket_port = test_port - 1
        # then override
        config.Webtools.port = test_port

        server = Root(config)
        server.start(blocking=False)
        self.assertEqual(cpconfig['server.socket_port'], test_port)
        server.stop()

    def testSecuritySetting(self):
        testRole = "TestRole"
        testGroup = "TestGroup"
        testSite = "TestSite"
        config = self.getBaseConfiguration()
        config.SecurityModule.dangerously_insecure = False
        # not real keyfile but for the test.
        # file will be deleted automaticall when garbage collected.
        tempFile = NamedTemporaryFile()
        config.SecurityModule.key_file = tempFile.name
        config.SecurityModule.section_("default")
        config.SecurityModule.default.role = testRole
        config.SecurityModule.default.group = testGroup
        config.SecurityModule.default.site = testSite
        config.Webtools.environment = "production"
        server = Root(config)
        server.start(blocking=False)
        self.assertEqual(cpconfig['tools.secmodv2.on'], True)
        self.assertEqual(cpconfig['tools.secmodv2.role'], testRole)
        self.assertEqual(cpconfig['tools.secmodv2.group'], testGroup)
        self.assertEqual(cpconfig['tools.secmodv2.site'], testSite)
        server.stop()

    def testInstanceInUrl(self):
        config = self.getBaseConfiguration()
        config.SecurityModule.dangerously_insecure = True
        server = Root(config)
        # Add our test page
        config.UnitTests.instances = ['foo', 'bar', 'baz/zoink']
        active = config.UnitTests.views.section_('active')
        active.section_('test')
        active.test.object = 'WMCore_t.WebTools_t.InstanceTestPage'
        active.test.section_('database')
        db_instances = active.test.database.section_('instances')
        foo = db_instances.section_('foo')
        bar = db_instances.section_('bar')
        baz = db_instances.section_('baz/zoink')
        foo.connectUrl = 'sqlite:///foo'
        bar.connectUrl = 'sqlite:///bar'
        baz.connectUrl = 'sqlite:///baz/zoink'
        active.test.section_('security')
        security_instances = active.test.security.section_('instances')
        sec_foo = security_instances.section_('foo')
        sec_bar = security_instances.section_('bar')
        sec_baz = security_instances.section_('baz/zoink')
        sec_foo.sec_params = 'test_foo'
        sec_bar.sec_params = 'test_bar'
        sec_baz.sec_params = 'test_baz'

        server.start(blocking=False)

        for instance in config.UnitTests.instances:
            url = 'http://127.0.0.1:%s/unittests/%s/test' % (cpconfig['server.socket_port'], instance)
            html = urllib.request.urlopen(url).read()
            self.assertEqual(html, instance)
            db_url = '%s/database' % url
            html = urllib.request.urlopen(db_url).read()
            self.assertEqual(html, db_instances.section_(instance).connectUrl)
            sec_url = '%s/security' % url
            html = urllib.request.urlopen(sec_url).read()
            self.assertEqual(html, security_instances.section_(instance).sec_params)
        server.stop()

    def testUsingFilterTool(self):
        """
        Use the filter tool to prevent unexpected accesses from
        unsupported methods
        TODO
        """
        pass

if __name__ == '__main__':
    unittest.main()
