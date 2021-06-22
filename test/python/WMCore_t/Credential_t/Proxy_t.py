#!/usr/bin/env python
"""
_Proxy_t_
Test the basic Proxy operations.
You need to source your UI before running these tests.
Your proxy is initialized in testAAACreateProxy method and it is used by the remaining tests.
"""
from __future__ import print_function
from __future__ import division

from past.utils import old_div
import unittest
import os
import logging
import logging.config
import time
import subprocess

from nose.plugins.attrib import attr
from WMCore.Credential.Proxy import Proxy
from Utils.PythonVersion import PY3
from Utils.Utilities import decodeBytesToUnicode

# You may have to set these environment variables to run in a local environment

group = os.environ.get('PROXY_GROUP', '')
role = os.environ.get('PROXY_ROLE', 'NULL')
myProxySvr = os.environ.get('MYPROXY_SERVER', 'myproxy.cern.ch')

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
        self.dict = {'logger': self.logger,
                     'vo': 'cms', 'group': group, 'role': role, 'myProxySvr': myProxySvr,
                     'proxyValidity' : '192:00', 'min_time_left' : 36000}

        self.proxyPath = None
        self.proxy = Proxy( self.dict )

    def tearDown(self):
        """
        _tearDown_

        Tear down the proxy.
        """
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

        stdout, _ = vomsProxyInfoCall.communicate()
        stdout = decodeBytesToUnicode(stdout) if PY3 else stdout
        return stdout[0:-1]

    def getUserAttributes(self):
        """
        _getUserAttributes_
        Retrieve the user's attributes from the voms-proxy-info call.
        """
        vomsProxyInfoCall = subprocess.Popen(["voms-proxy-info", "-fqan"],
                                             stdout = subprocess.PIPE,
                                             stderr = subprocess.PIPE)
        if vomsProxyInfoCall.wait() != 0:
            return None

        stdout, _ = vomsProxyInfoCall.communicate()
        stdout = decodeBytesToUnicode(stdout) if PY3 else stdout
        return stdout[0:-1]

    @attr("integration")
    def testGetUserCertEnddate( self ):
        """
        Test if getTimeLeft method returns correctly the proxy time left.
        """
        daysleft = self.proxy.getUserCertEnddate()
        self.assertEqual(daysleft, 58) #set this as the number of days left in .globus/usercert.pem
        daysleft = self.proxy.getUserCertEnddate(openSSL=False)
        self.assertEqual(daysleft, 58) #set this as the number of days left in .globus/usercert.pem

    @attr("integration")
    def testAAACreateProxy( self ):
        """
        Test if create method creates correctly the proxy.
        This is sort of bad form to require that this test run first, but the alternative is
        entering a password for every single invocation
        """
        self.proxy.create()
        time.sleep( 5 )
        proxyPath = self.proxy.getProxyFilename()
        self.assertTrue(os.path.exists(proxyPath))

    @attr("integration")
    def testCheckProxyTimeLeft( self ):
        """
        Test if getTimeLeft method returns correctly the proxy time left.
        """
        timeLeft = self.proxy.getTimeLeft()
        self.assertEqual(int(int(timeLeft) // 3600), 191)

    @attr("integration")
    def testRenewProxy( self ):
        """
        Test if the renew method renews correctly the user proxy.
        """
        time.sleep( 70 )
        self.proxy.renew()
        time.sleep( 10 )
        timeLeft = self.proxy.getTimeLeft()
        self.assertEqual(int(int(timeLeft) // 3600), 191)

    @attr("integration")
    def testDestroyProxy(self ):
        """
        Test the proxy destroy method.
        """
        self.proxy.destroy( )
        self.proxyPath = self.proxy.getProxyFilename()
        self.assertFalse(os.path.exists(self.proxyPath))
        # Create the proxy after the destroy
        self.proxy.create()

    @attr("integration")
    def testGetSubject(self):
        """
        _testGetSubject_
        Verify that the getSubject() method works correctly.
        """
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
        user = self.proxy.getUserName( )
        identity = self.getUserIdentity().split("/")[ len(self.getUserIdentity().split("/")) - 1 ][3:]
        self.assertEqual(user, identity,
                         "Error: User name is wrong: |%s|\n|%s|" % (user, identity))
        return

    @attr("integration")
    def testCheckAttribute( self ):
        """
        Test if the checkAttribute  method checks correctly the attributes validity.
        """
        valid = self.proxy.checkAttribute( )
        self.assertTrue(valid)

    @attr("integration")
    def testCheckTimeLeft( self ):
        """
        Test if the check method checks correctly the proxy validity.
        """
        valid = self.proxy.check( self.proxyPath )
        self.assertTrue(valid)

    @attr("integration")
    def testVomsRenewal( self ):
        """
        Test if vomsExtensionRenewal method renews correctly the voms-proxy.
        """
        proxyPath = self.proxy.getProxyFilename( )
        time.sleep( 70 )
        attribute = self.proxy.prepareAttForVomsRenewal( self.proxy.getAttributeFromProxy( proxyPath ) )
        self.proxy.vomsExtensionRenewal( proxyPath, attribute )
        vomsTimeLeft = self.proxy.getVomsLife( proxyPath )
        self.assertEqual(int(int(vomsTimeLeft) // 3600), 191)

    @attr("integration")
    def testElevateAttribute( self ):
        """
        Test if the vomsExtensionRenewal method elevate last attributes given.
        """
        proxyPath = self.proxy.getProxyFilename( )
        attribute = self.proxy.prepareAttForVomsRenewal( '/cms/Role=NULL/Capability=NULL' )
        self.proxy.vomsExtensionRenewal( proxyPath, attribute )
        self.assertEqual(self.proxy.getAttributeFromProxy(proxyPath), '/cms/Role=NULL/Capability=NULL')
        # Restore the original configuration of the proxy
        self.proxy.create()

    @attr("integration")
    def testUserGroupInProxy( self ):
        """
        Test if getUserAttributes method returns correctly the user group.
        """
        self.assertTrue(self.proxy.group, 'No group set. Testing incomplete.')
        self.assertEqual(self.proxy.group, self.getUserAttributes().split('\n')[0].split('/')[2])

    @attr("integration")
    def testUserRoleInProxy( self ):
        """
        Test if getUserAttributes method returns correctly the user role.
        """
        self.assertEqual(self.proxy.role, self.getUserAttributes().split('\n')[0].split('/')[3].split('=')[1])

    @attr("integration")
    def testGetAttributes( self ):
        """
        Test getAttributeFromProxy method.

        Can tested this with:
            voms-proxy-init -voms cms:/cms/integration #or any group of yours
            export PROXY_GROUP=integration
            python test/python/WMCore_t/Credential_t/Proxy_t.py ProxyTest.testGetAttributes
        """
        self.assertTrue(self.proxy.group, 'No group set. Testing incomplete.')
        if not self.dict['role']:
            role = 'NULL'
        else:
            role = self.dict['role']
        proxyPath = self.proxy.getProxyFilename( )
        self.assertEqual(self.proxy.getAttributeFromProxy(proxyPath).split('/')[2], self.dict['group'])
        self.assertEqual(self.proxy.getAttributeFromProxy(proxyPath).split('/')[3].split('=')[1], role)

        #test with the allAttributes flag
        self.assertTrue(self.proxy.getAttributeFromProxy(proxyPath, allAttributes=True)>1)

    @attr("integration")
    def testGetUserGroupAndRole( self ):
        """
        Test GetUserGroupAndRoleFromProxy method.
        """
        if not self.dict['role']:
            role = 'NULL'
        else:
            role = self.dict['role']
        proxyPath = self.proxy.getProxyFilename( )
        if self.dict['group'] and self.dict['role']:
            self.assertEqual(self.proxy.getUserGroupAndRoleFromProxy(proxyPath)[0], self.dict['group'])
            self.assertEqual(self.proxy.getUserGroupAndRoleFromProxy(proxyPath)[1], role)

    @attr("integration")
    def testGetAllUserGroups( self ):
        """
        Test GetAllUserGroups method.
        """
        proxyPath = self.proxy.getProxyFilename( )
        groups = self.proxy.getAllUserGroups(proxyPath)
        print(list(groups))


if __name__ == '__main__':
    unittest.main()
