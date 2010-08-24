#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
Component test TestComponent module and the harness
"""

__revision__ = "$Id: Harness_t.py,v 1.10 2009/02/27 22:19:22 fvlingen Exp $"
__version__ = "$Revision: 1.10 $"
__author__ = "fvlingen@caltech.edu"

import commands
import logging
import os
import threading
import time
import unittest

from WMCore_t.Agent_t.TestComponent import TestComponent

from WMCore.Agent.Configuration import Configuration
from WMCore.Agent.Daemon.Create import createDaemon
from WMCore.Agent.Daemon.Details import Details
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMQuality.TestInit import TestInit

class HarnessTest(unittest.TestCase):
    """
    TestCase for TestComponent module 
    """

    _setup_done = False
    _teardown = False
    _log_level = 'debug'

    def setUp(self):
        """
        setup for test.
        """
        if not HarnessTest._setup_done:
            self.testInit = TestInit(__file__)
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            self.testInit.setSchema()
            HarnessTest._setup_done = True

    def tearDown(self):
        """
        Delete database 
        """
        myThread = threading.currentThread()
        if HarnessTest._teardown and myThread.dialect == 'MySQL':
            command = 'mysql -u root --socket='\
            + os.getenv('TESTDIR') \
            + '/mysqldata/mysql.sock --exec "drop database ' \
            + os.getenv('DBNAME')+ '"'
            commands.getstatusoutput(command)

            command = 'mysql -u root --socket=' \
            + os.getenv('TESTDIR')+'/mysqldata/mysql.sock --exec "' \
            + os.getenv('SQLCREATE') + '"'
            commands.getstatusoutput(command)

            command = 'mysql -u root --socket=' \
            + os.getenv('TESTDIR') \
            + '/mysqldata/mysql.sock --exec "create database ' \
            +os.getenv('DBNAME')+ '"'
            commands.getstatusoutput(command)
        HarnessTest._teardown = False

    def setConfig(self):
        config = Configuration()
        config.Agent.contact = "fvlingen@caltech.edu"
        config.Agent.teamName = "Lakers"
        config.Agent.agentName = "Lebron James"

        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR")

        config.component_("TestComponent")
        config.TestComponent.logLevel = 'INFO'

        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql' 
        config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.CoreDatabase.user = os.getenv("DBUSER")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        config.CoreDatabase.hostname = os.getenv("DBHOST")
        config.CoreDatabase.name = os.getenv("DBNAME")
        return config


    def testA(self):
        """
        Mimics creation of component and handles come messages.
        """
        # we want to read this from a file for the actual components.
        config = self.setConfig()
        testComponent = TestComponent(config)
        testComponent.prepareToStart()

        testComponent.handleMessage('LogState','')
        testComponent.handleMessage('TestMessage1','TestMessag1Payload')
        testComponent.handleMessage('TestMessage2','TestMessag2Payload')
        testComponent.handleMessage('TestMessage3','TestMessag3Payload')
        testComponent.handleMessage('TestMessage4','TestMessag4Payload')
        testComponent.handleMessage('Logging.DEBUG','')
        testComponent.handleMessage('Logging.WARNING','')
        testComponent.handleMessage('Logging.CRITICAL','')
        testComponent.handleMessage('Logging.ERROR','')
        testComponent.handleMessage('Logging.INFO','')
        testComponent.handleMessage('Logging.SQLDEBUG','')
        testComponent.handleMessage('TestComponent:Logging.DEBUG','')
        testComponent.handleMessage('TestComponent:Logging.WARNING','')
        testComponent.handleMessage('TestComponent:Logging.CRITICAL','')
        testComponent.handleMessage('TestComponent:Logging.ERROR','')
        testComponent.handleMessage('TestComponent:Logging.INFO','')
        testComponent.handleMessage('TestComponent:Logging.SQLDEBUG','')
        # test a non existing message (to generate an error)
        errorMsg = ''
        try:
            testComponent.handleMessage('NonExistingMessageType','')
        except Exception,ex:
            errorMsg = str(ex)
        assert errorMsg.startswith('Message NonExistingMessageType with payload')

        HarnessTest._setup_done = False
        HarnessTest._teardown = True

    def testB(self):

        config = self.setConfig()
        # as this is a test we build the string from our global environment
        # parameters normally you put this straight into the DefaultConfig.py file:

        config.CoreDatabase.connectUrl = os.getenv("DATABASE")
        # make the other parameters none, to ensure we testing the right connection:
        config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.CoreDatabase.user = None
        config.CoreDatabase.passwd = None
        config.CoreDatabase.hostname = None
        config.CoreDatabase.name = None

        testComponent = TestComponent(config)
        testComponent.prepareToStart()

        testComponent.handleMessage('LogState','')
        testComponent.handleMessage('TestMessage1','TestMessag1Payload')
        testComponent.handleMessage('TestMessage2','TestMessag2Payload')
        testComponent.handleMessage('TestMessage3','TestMessag3Payload')
        testComponent.handleMessage('TestMessage4','TestMessag4Payload')
        testComponent.handleMessage('Logging.DEBUG','')
        testComponent.handleMessage('Logging.WARNING','')
        testComponent.handleMessage('Logging.CRITICAL','')
        testComponent.handleMessage('Logging.ERROR','')
        testComponent.handleMessage('Logging.INFO','')
        testComponent.handleMessage('Logging.SQLDEBUG','')
        testComponent.handleMessage('TestComponent:Logging.DEBUG','')
        testComponent.handleMessage('TestComponent:Logging.WARNING','')
        testComponent.handleMessage('TestComponent:Logging.CRITICAL','')
        testComponent.handleMessage('TestComponent:Logging.ERROR','')
        testComponent.handleMessage('TestComponent:Logging.INFO','')
        testComponent.handleMessage('TestComponent:Logging.SQLDEBUG','')
        # test a non existing message (to generate an error)
        errorMsg = ''
        try:
            testComponent.handleMessage('NonExistingMessageType','')
        except Exception,ex:
            errorMsg = str(ex)
        assert errorMsg.startswith('Message NonExistingMessageType with payload')

        HarnessTest._setup_done = False
        HarnessTest._teardown = True

    def testC(self):
        config = self.setConfig()
        # try starting a component as a deamon:
        config.TestComponent.componentDir = os.path.join(config.General.workDir, "Components/TestComponent1")
        testComponent = TestComponent(config)
        # we set the parent to true as we are testing
        testComponent.startDeamon(keepParent = True)
        print('trying to kill the component')
        time.sleep(2)
        daemonFile = os.path.join(config.TestComponent.componentDir, "Daemon.xml")
        details = Details(daemonFile)
        print('Is component alive: '+str(details.isAlive()))
        time.sleep(2)
        details.killWithPrejudice()
        print('Daemon killed')

        HarnessTest._setup_done = False
        HarnessTest._teardown = True

    def testD(self):
        config = self.setConfig()
        # try starting a component as a deamon:
        config.TestComponent.componentDir = os.path.join(config.General.workDir, "Components/TestComponent2")
        testComponent = TestComponent(config)
        # we set the parent to true as we are testing
        testComponent.startDeamon(keepParent = True)
        time.sleep(2)
        daemonFile = os.path.join(config.TestComponent.componentDir, "Daemon.xml")
        details = Details(daemonFile)
        print('Is component alive: '+str(details.isAlive()))

        #create msgService to send stop message.
        myThread = threading.currentThread()
        factory = WMFactory("msgService", "WMCore.MsgService."+ \
            myThread.dialect)
        myThread.transaction = Transaction(myThread.dbi)
        msgService = factory.loadObject("MsgService")
        msgService.registerAs("HarnessTest")
        myThread.transaction.commit()

        print('Publish a stop message to test if the component shutsdown gracefully')
        myThread.transaction.begin()
        msg = {'name' : 'Stop', 'payload' : ''}
        msgService.publish(msg)
        myThread.transaction.commit()

        msgService.finish()

        while details.isAlive():
            print('Component has not received stop message')
            time.sleep(2)
        print('Daemon shutdown gracefully')

        HarnessTest._teardown = True

    def runTest(self):
        """
        Tests the harness.
        """

        self.testA()
        self.testB()
        self.testC()
        self.testD()

if __name__ == '__main__':
    unittest.main()

