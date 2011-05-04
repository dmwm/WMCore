#!/bin/env python
"""Tests for CouchProxyManager"""

import unittest
import os
import threading
import urllib
import time

from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMCore.DAOFactory import DAOFactory

from WMComponent.CouchProxyManager.CouchProxyManager import CouchProxyRunner, get_logger

#FIXME: Currently there are no tests that verify the x509 handling,
# work out how to handle these in buildbot etc.

#FIXME: (Before adding a new test) Need to stop thread at the end of each test

class CouchProxyManagerTest(unittest.TestCase):
    """Tests for CouchProxyManager"""
    def setUp(self):
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()

    def tearDown(self):
        self.testInit.tearDownCouch()

    def testContactCouch(self):
        """Can contact a local couch server"""
        logger = get_logger(False, None)
        dest = os.environ['COUCHURL']
        proxy = CouchProxyRunner(logger = logger, remote_host = dest)
        proxy_thread = threading.Thread(target = proxy.proxy.run)
        proxy_thread.daemon = True
        proxy_thread.start()
        time.sleep(0.01) # bad i know, but we need the server to start
        output = urllib.urlopen('http://127.0.0.1:8080').read()
        self.assertTrue(output.find('Welcome') > -1)

if __name__ == "__main__":
    unittest.main()
