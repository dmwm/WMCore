#!/usr/bin/env python
"""
_SimpleMyProxy_t_
Test the two basic SimpleMyProxy operations.
There is NO need to source your UI before running these tests.
The SimpleMyProxy module has not ability to delegate/create proxy/myproxy so it assumes a myproxy already exists.
"""

import unittest
import os
import logging
import re

from nose.plugins.attrib import attr
from WMCore.Credential.SimpleMyProxy import SimpleMyProxy

MYPROXYSERVER = os.environ.get('MYPROXY_SERVER', 'myproxy.cern.ch')
MYPROXYPORT = 7512
SERVERDN = os.environ.get('SERVER_DN', '/DC=ch/DC=cern/OU=computers/CN=mattia-dev02.cern.ch')
SERVERKEY = os.environ.get('SERVER_KEY', '/data/certs/hostkey.pem')
SERVERCERT = os.environ.get('SERVER_CERT', '/data/certs/hostcert.pem')
USERKEY = os.environ.get('USER_KEY', os.path.join(os.environ.get('HOME'),'.globus/userkey.pem'))
USERCERT = os.environ.get('USER_CERT', os.path.join(os.environ.get('HOME'),'.globus/usercert.pem'))
USERNAME = "simple_myproxy_test"

REGCERT = re.compile(r"^[-]{5}BEGIN CERTIFICATE[-]{5}[a-zA-Z0-9\\\n \+-]+")

class SimpleMyProxyTest(unittest.TestCase):

    def setUp(self):
        """
        Setup for unit tests
        """
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='proxy_unittests.log',
                            filemode='w')
        logger_name = 'SimpleMyProxyTest'
        self.logger = logging.getLogger(logger_name)
        self.simplemyproxy = SimpleMyProxy({'logger': self.logger})

    def tearDown(self):
        """
        _tearDown_
        """
        return

    @attr("integration")
    def testgetMyProxyInfo(self):
        """
        Test if it's possible to get myproxy information
        """
        myproxyinfo = self.simplemyproxy.checkMyProxy(username=USERNAME, myproxyserver=MYPROXYSERVER, myproxyport=MYPROXYPORT,
                                                      keyfile=USERKEY, certfile=USERCERT)
        assert 'retriever' in myproxyinfo
        assert 'owner' in myproxyinfo
        assert 'start' in myproxyinfo
        assert 'end' in myproxyinfo
        assert myproxyinfo['end'] > myproxyinfo['start']

    @attr("integration")
    def testgetMyProxy(self):
        """
        Test if it is possible to retrieve a proxy from an existing myproxy
        """
        myproxy = self.simplemyproxy.logonRenewMyProxy(username=USERNAME, myproxyserver=MYPROXYSERVER, myproxyport=MYPROXYPORT,
                                                       keyfile=SERVERKEY, certfile=SERVERCERT, lifetime=43200)
        self.assertTrue(REGCERT.match(myproxy))


if __name__ == '__main__':
    unittest.main()
