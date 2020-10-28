#!/usr/bin/env python
"""
_MyProxy_t_
Test the basic MyProxy operations.
You need to source your UI before running these tests.
The user Proxy and MyProxy is initialized in testCreateMyProxy method and they are used by the remaining tests.
"""
from __future__ import division

import unittest
import os
import logging
import logging.config
import time

from nose.plugins.attrib import attr
from WMCore.Credential.Proxy import Proxy, myProxyEnvironment

# You may have to change these variables to run in a local environment
group = os.environ.get('PROXY_GROUP', '')
role = os.environ.get('PROXY_ROLE', 'NULL')
myProxySvr = os.environ.get('MYPROXY_SERVER', 'myproxy.cern.ch')
uiPath = os.environ.get('GLITE_UI', '/afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh')

serverDN = os.environ.get('SERVER_DN', '/C=IT/O=INFN/OU=Host/L=Perugia/CN=crab.pg.infn.it')
serverKey = os.environ.get('SERVER_KEY', '/path/to/key.pem')
serverCert = os.environ.get('SERVER_CERT', '/path/to/cert.pem')

class MyProxyTest(unittest.TestCase):

    def setUp(self):
        """
        Setup for unit tests
        """
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename='proxy_unittests.log',
                            filemode='w')
        logger_name = 'ProxyTest'
        self.logger = logging.getLogger(logger_name)
        self.dict = {'logger': self.logger, 'vo': 'cms', 'group': group, 'role': role,
                     'myProxySvr': myProxySvr, 'proxyValidity' : '192:00', 'min_time_left' : 36000,
                     'uisource' : uiPath, 'serverDN' : serverDN}

        self.proxyPath = None
        self.proxy = Proxy( self.dict )
        self.serverDN = self.dict['serverDN']

    def tearDown(self):
        """
        _tearDown_
        """
        return

    @attr("integration")
    def testAAACreateMyProxy( self ):
        """
        Test if delegate method create correctly the MyProxy.
        """
        self.proxy.create()
        self.proxy.delegate( credential = self.proxyPath )
        valid = self.proxy.checkMyProxy( )
        self.assertTrue(valid, 'Could not create MyProxy')

    @attr("integration")
    def testDelegateServer( self ):
        """
        Test if delegate method create MyProxy and delegate
        the retrieval to the server correctly.
        """
        self.proxy.delegate( credential = self.proxyPath, serverRenewer = True )
        valid = self.proxy.checkMyProxy( checkRenewer = True )
        self.assertTrue(valid)

    @attr("integration")
    def testCheckMyProxy( self ):
        """
        Test if checkMyProxy checks correctly the MyProxy validity.
        """
        valid = self.proxy.checkMyProxy( )
        self.assertTrue(valid)

    @attr("integration")
    def testRenewMyProxy( self ):
        """
        Test if renewMyProxy method renews correctly the MyProxy.
        """
        self.proxy.renewMyProxy( proxy = self.proxyPath )
        time.sleep( 5 )
        timeLeft = self.proxy.getMyProxyTimeLeft( proxy = self.proxyPath )
        self.assertEqual(int(int(timeLeft) // 3600), 167)

    @attr("integration")
    def testRenewMyProxyForServer( self ):
        """
        Renew MyProxy which the retrieval is delegated to a server.
        """
        time.sleep( 70 )
        self.proxy.renewMyProxy( proxy = self.proxyPath, serverRenewer = True )
        time.sleep( 5 )
        timeLeft = self.proxy.getMyProxyTimeLeft( proxy = self.proxyPath, serverRenewer = True )
        self.assertEqual(int(int(timeLeft) // 3600), 167)

    @attr("integration")
    def testMyProxyEnvironment(self):
        """
        Test the myProxyEnvironment context manager
        In this test a new Proxy and MyProxy are initialized
        """
        myProxy = Proxy(self.dict)

        # Create the proxy
        myProxy.create()
        proxyPath = myProxy.getProxyFilename()
        userDN = myProxy.getSubject()
        self.assertTrue(os.path.exists(proxyPath))

        # Delegate and check the proxy
        myProxy.delegate(credential=proxyPath, serverRenewer=True)
        valid = myProxy.checkMyProxy()
        self.assertTrue(valid)

        # Make sure X509_USER_PROXY exists only in the context manager and corresponds to a file
        if 'X509_USER_PROXY' in os.environ:
            del os.environ['X509_USER_PROXY']
        self.assertFalse('X509_USER_PROXY' in os.environ)
        with myProxyEnvironment(userDN=userDN, serverCert=serverCert, serverKey=serverKey,
                                myproxySrv='myproxy.cern.ch', proxyDir='/tmp/', logger=self.logger):
            self.assertTrue('X509_USER_PROXY' in os.environ)
            self.assertTrue(os.path.exists(os.environ['X509_USER_PROXY']))
        self.assertFalse('X509_USER_PROXY' in os.environ)

        return

if __name__ == '__main__':
    unittest.main()
