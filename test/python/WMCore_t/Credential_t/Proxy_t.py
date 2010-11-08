"""
"""
import unittest
import os
import logging
import logging.config
import socket
import time
import tempfile
from Proxy import Proxy

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
        dict = { 'logger': self.logger,
                'vo':'cms', 'myProxySvr':'myproxy.cern.ch',
                'proxyValidity' : '192:00', 'min_time_left' : 36000, 
#                'serverDN' : '/C=IT/O=INFN/OU=Host/L=Bari/CN=crab1.ba.infn.it', 
                'serverDN' : '/C=IT/O=INFN/OU=Host/L=Pisa/CN=crabserv.pi.infn.it',
                'userDN' : '/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi' }

        self.userDN = '/C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Hassen Riahi' 
        self.userName = 'Hassen Riahi'
        self.proxyPath = None
        self.proxy = Proxy( dict )
        self.serverKey = '/home/crab/.globus/hostcert.pem'

    def testDestroyBeforeCreation(self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.destroy( )
           self.proxyPath = self.proxy.getProxyFilename()
           assert not os.path.exists(self.proxyPath)

    def testCreateProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.create()
           time.sleep( 5 )
           proxyPath = self.proxy.getProxyFilename()
           assert os.path.exists(proxyPath)


    def testCheckProxyTimeLeft( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           timeLeft = self.proxy.getTimeLeft()
           assert ( int(timeLeft) / 3600 ) == 191

    def testRenewProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           time.sleep( 70 )

           self.proxy.renew()
           time.sleep( 10 )
           timeLeft = self.proxy.getTimeLeft()

           assert ( int(timeLeft) / 3600 ) == 191

    def testDestroyProxy(self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.destroy( )
           self.proxyPath = self.proxy.getProxyFilename()
           assert not os.path.exists(self.proxyPath)

    def testGetSubject( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.testCreateProxy()
           subject = self.proxy.getSubject( )

           assert subject == self.userDN 

    def testGetUserName( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           user = self.proxy.getUserName( )
           assert user == self.userName

    def checkAttribute( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           valid = self.proxy.checkAttribute( )
           assert valid == True 


    def testCheckTimeLeft( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           valid = self.proxy.check( self.proxyPath )
           assert valid == True 

    def testDelegateMyProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.delegate( credential = self.proxyPath )
           valid = self.proxy.checkMyProxy( )
           assert valid == True 

    def testDelegateServerAndMyProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.delegate( credential = self.proxyPath, serverRenewer = True )
           valid = self.proxy.checkMyProxy( checkRenewer = True )
           assert valid == True

    def testCheckMyProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.delegate( )
           valid = self.proxy.checkMyProxy( )
           assert valid == True

    def testCheckMyProxyServer( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           self.proxy.delegate( serverRenewer = True )
           valid = self.proxy.checkMyProxy( checkRenewer = True )
           assert valid == True


    def testLogonRenewMyProxy( self ):
        """
       """
        if os.path.exists( self.serverKey ):

           proxyFile = self.proxy.logonRenewMyProxy( )
           assert os.path.exists( proxyFile )
        

    def testRenewMyProxy( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           time.sleep( 70 )
           self.proxy.renewMyProxy( proxy = self.proxyPath )
           time.sleep( 5 )
           timeLeft = self.proxy.getMyProxyTimeLeft( proxy = self.proxyPath )

           print ( int(timeLeft) / 3600 )
 
           assert ( int(timeLeft) / 3600 ) == 167

    def testRenewMyProxyForServer( self ):
        """
        """
        if not os.path.exists( self.serverKey ):

           time.sleep( 70 )
           self.proxy.renewMyProxy( proxy = self.proxyPath, serverRenewer = True )
           time.sleep( 5 )
           timeLeft = self.proxy.getMyProxyTimeLeft( proxy = self.proxyPath, serverRenewer = True )
           print ( int(timeLeft) / 3600 ) 
           assert ( int(timeLeft) / 3600 ) == 167

    def testRenewMyProxyByServer( self ):
        """
        """
        if os.path.exists( self.serverKey ):

           proxyPath = self.proxy.getProxyFilename( serverRenewer = True )
           self.proxy.logonRenewMyProxy( proxyPath )
           timeLeft = self.proxy.getTimeLeft( proxyPath )
           print ( int(timeLeft) / 3600 ) 
           assert ( int(timeLeft) / 3600 ) > 120

#    def testDestroyMyProxy( self ):
#        """
#        """
#         return 

if __name__ == '__main__':
    unittest.main()

