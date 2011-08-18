#!/usr/bin/env python
"""
_Proxy_t_

"""

import unittest
import os
import logging
import logging.config
import socket
import time
import tempfile
import subprocess

from nose.plugins.attrib import attr

from WMCore.Credential.Proxy import Proxy

class ProxyTest(unittest.TestCase):

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
        dict = {'logger': self.logger,
                'server_key' : '/home/crab/.globus/hostkey.pem', 'server_cert' : '/home/crab/.globus/hostcert.pem',  
                'vo': 'cms', 'myProxySvr': 'myproxy.cern.ch',
                'proxyValidity' : '192:00', 'min_time_left' : 36000}

        self.proxyPath = None
        self.proxy = Proxy( dict )
        self.serverKey = dict['server_key']
        self.serverDN = None
        if dict.has_key('serverDN'): self.serverDN = dict['serverDN']

    def tearDown(self):
        """
        _tearDown_

        Tear down the proxy.
        """
        self.proxy.destroy()
        return

    def getUserIdentity(self):
        """
        _getUserIdentity_

        Retrieve the user's subject from the voms-proxy-info call.
        """
        vomsProxyInfoCall = subprocess.Popen(["voms-proxy-info", "-identity"],
                                             stdout = subprocess.PIPE,
                                             stderr = subprocess.PIPE)
        if vomsProxyInfoCall.wait() != 0:
            return None
        
        (stdout, stderr) = vomsProxyInfoCall.communicate()
        return stdout[0:-1]

    @attr("integration")
    def testDestroyBeforeCreation(self ):
        """
        """
        if not os.path.exists( self.serverKey ):

            self.proxy.destroy( )
            self.proxyPath = self.proxy.getProxyFilename()
            assert not os.path.exists(self.proxyPath)

    @attr("integration")
    def testCreateProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):
            self.proxy.create()
            time.sleep( 5 )
            proxyPath = self.proxy.getProxyFilename()
            assert os.path.exists(proxyPath)

    @attr("integration")
    def testCheckProxyTimeLeft( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.create()
           timeLeft = self.proxy.getTimeLeft()
           assert ( int(timeLeft) / 3600 ) == 192

    @attr("integration")
    def testRenewProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           time.sleep( 70 )

           self.proxy.renew()
           time.sleep( 10 )
           timeLeft = self.proxy.getTimeLeft()

           assert ( int(timeLeft) / 3600 ) == 191

    @attr("integration")
    def testDestroyProxy(self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.destroy( )
           self.proxyPath = self.proxy.getProxyFilename()
           assert not os.path.exists(self.proxyPath)

    @attr("integration")
    def testGetSubject(self):
        """
        _testGetSubject_
        
        Verify that the getSubject() method works correctly.
        """
        if os.path.exists(self.serverKey):
            return
        
        self.testCreateProxy()
        subject = self.proxy.getSubject( )
        
        self.assertEqual(subject, self.getUserIdentity(),
                         "Error: Wrong subject.")
        return

    @attr("integration")
    def testGetUserName( self ):
        """
        _testGetUserName_

        Verify that the getUserName() method correctly determines the user's
        name.
        """
        if os.path.exists( self.serverKey ):
            return

        self.testCreateProxy()
        user = self.proxy.getUserName( )
        identity = self.getUserIdentity().split("/")[ len(self.getUserIdentity().split("/")) - 1 ][3:]

        self.assertEqual(user, identity,
                         "Error: User name is wrong: |%s|\n|%s|" % (user, identity))
        return

    @attr("integration")
    def checkAttribute( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           valid = self.proxy.checkAttribute( )
           assert valid == True 

    @attr("integration")
    def testCheckTimeLeft( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           valid = self.proxy.check( self.proxyPath )
           assert valid == True 

    @attr("integration")
    def testDelegateMyProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.create()
           self.proxy.delegate( credential = self.proxyPath )
           valid = self.proxy.checkMyProxy( )
           assert valid == True 

    @attr("integration")
    def testDelegateServerAndMyProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.create()
           self.proxy.delegate( credential = self.proxyPath, serverRenewer = True )
           valid = self.proxy.checkMyProxy( checkRenewer = True )
           assert valid == True

    @attr("integration")
    def testCheckMyProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.create()
           self.proxy.delegate( )
           valid = self.proxy.checkMyProxy( )
           assert valid == True

    @attr("integration")
    def testCheckMyProxyServer( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.create()
           self.proxy.delegate( serverRenewer = True )
           valid = self.proxy.checkMyProxy( checkRenewer = True )
           assert valid == True

    @attr("integration")
    def testLogonRenewMyProxy( self ):
        """
       """
        if os.path.exists( self.serverKey ):

           proxyFile = self.proxy.logonRenewMyProxy( )
           assert os.path.exists( proxyFile )
        
    @attr("integration")
    def testRenewMyProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.create()
           time.sleep( 70 )
           self.proxy.renewMyProxy( proxy = self.proxyPath )
           time.sleep( 5 )
           timeLeft = self.proxy.getMyProxyTimeLeft( proxy = self.proxyPath )

           assert ( int(timeLeft) / 3600 ) == 167

    @attr("integration")
    def testRenewMyProxyForServer( self ):
        """
        """
        if not os.path.exists( self.serverKey ) and self.serverDN:

            self.proxy.create()
            time.sleep( 70 )
            self.proxy.renewMyProxy( proxy = self.proxyPath, serverRenewer = True )
            time.sleep( 5 )
            timeLeft = self.proxy.getMyProxyTimeLeft( proxy = self.proxyPath, serverRenewer = True )
            assert ( int(timeLeft) / 3600 ) == 167

    @attr("integration")
    def testRenewMyProxyByServer( self ):
        """
        """
        if os.path.exists( self.serverKey ):

           proxyPath = self.proxy.getProxyFilename( serverRenewer = True )
           self.proxy.logonRenewMyProxy( proxyPath )
           timeLeft = self.proxy.getTimeLeft( proxyPath )
           assert ( int(timeLeft) / 3600 ) > 120

    @attr("integration")
    def testVomsRenewal( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.create()
           proxyPath = self.proxy.getProxyFilename( )

           time.sleep( 70 )

           self.proxy.vomsExtensionRenewal( proxyPath )
           vomsTimeLeft = self.proxy.getVomsLife( proxyPath )
           assert ( int(vomsTimeLeft) / 3600 ) == 191

#    def testDestroyMyProxy( self ):
#        """
#        """
#         return 

if __name__ == '__main__':
    unittest.main()

