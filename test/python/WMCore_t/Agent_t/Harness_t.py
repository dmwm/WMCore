#!/usr/bin/env python
# pylint: disable=E1101,C0103,R0902
"""
Component test TestComponent module and the harness
"""
from __future__ import print_function

import os
import threading
import time
import unittest

import nose

from WMCore.Agent.Daemon.Details import Details
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory
from WMCore_t.Agent_t.TestComponent import TestComponent
from WMQuality.TestInit import TestInit


class HarnessTest(unittest.TestCase):
    """
    TestCase for TestComponent module
    """
    tempDir = None

    def setUp(self):
        """
        setup for test.
        """
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setSchema()

    def tearDown(self):
        """
        Delete database
        """
        self.testInit.clearDatabase()

    def testB(self):
        raise nose.SkipTest
        config = self.testInit.getConfiguration()
        self.tempDir = self.testInit.generateWorkDir(config)
        config.component_("TestComponent")
        config.TestComponent.logLevel = 'INFO'
        config.section_("General")
        config.TestComponent.componentDir = os.path.join( \
            self.tempDir, "Components/TestComponent1")
        config.General.workDir = config.TestComponent.componentDir

        os.makedirs(config.TestComponent.componentDir)
        # as this is a test we build the string from our global environment
        # parameters normally you put this straight into the DefaultConfig.py file:
        # testInit.getConfiguration returns from the environment variable by default
        testComponent = TestComponent(config)
        testComponent.prepareToStart()

        testComponent.handleMessage('LogState', '')
        testComponent.handleMessage('TestMessage1', 'TestMessag1Payload')
        testComponent.handleMessage('TestMessage2', 'TestMessag2Payload')
        testComponent.handleMessage('TestMessage3', 'TestMessag3Payload')
        testComponent.handleMessage('TestMessage4', 'TestMessag4Payload')
        testComponent.handleMessage('Logging.DEBUG', '')
        testComponent.handleMessage('Logging.WARNING', '')
        testComponent.handleMessage('Logging.CRITICAL', '')
        testComponent.handleMessage('Logging.ERROR', '')
        testComponent.handleMessage('Logging.INFO', '')
        testComponent.handleMessage('Logging.SQLDEBUG', '')
        testComponent.handleMessage('TestComponent:Logging.DEBUG', '')
        testComponent.handleMessage('TestComponent:Logging.WARNING', '')
        testComponent.handleMessage('TestComponent:Logging.CRITICAL', '')
        testComponent.handleMessage('TestComponent:Logging.ERROR', '')
        testComponent.handleMessage('TestComponent:Logging.INFO', '')
        testComponent.handleMessage('TestComponent:Logging.SQLDEBUG', '')
        # test a non existing message (to generate an error)
        errorMsg = ''
        try:
            testComponent.handleMessage('NonExistingMessageType', '')
        except Exception as ex:
            errorMsg = str(ex)
        self.assertTrue(errorMsg.startswith('Message NonExistingMessageType with payload'))

    def testC(self):
        raise nose.SkipTest
        config = self.testInit.getConfiguration()
        self.tempDir = self.testInit.generateWorkDir(config)
        config.component_("TestComponent")
        config.TestComponent.logLevel = 'INFO'
        config.section_("General")
        # try starting a component as a daemon:
        config.TestComponent.componentDir = os.path.join( \
            self.tempDir, "Components/TestComponent1")
        os.makedirs(config.TestComponent.componentDir)
        testComponent = TestComponent(config)
        # we set the parent to true as we are testing
        testComponent.startDaemon(keepParent=True)
        print('trying to kill the component')
        time.sleep(2)
        daemonFile = os.path.join(config.TestComponent.componentDir, "Daemon.xml")
        details = Details(daemonFile)
        print('Is component alive: ' + str(details.isAlive()))
        time.sleep(2)
        details.killWithPrejudice()
        print('Daemon killed')

    def testD(self):
        raise nose.SkipTest
        config = self.testInit.getConfiguration()
        config.component_("TestComponent")
        config.TestComponent.logLevel = 'INFO'
        config.section_("General")
        self.tempDir = self.testInit.generateWorkDir(config)
        # try starting a component as a daemon:
        config.TestComponent.componentDir = os.path.join( \
            self.tempDir, "Components/TestComponent2")
        os.makedirs(config.TestComponent.componentDir)
        testComponent = TestComponent(config)
        # we set the parent to true as we are testing
        testComponent.startDaemon(keepParent=True)
        time.sleep(2)
        daemonFile = os.path.join(config.TestComponent.componentDir, "Daemon.xml")
        details = Details(daemonFile)
        print('Is component alive: ' + str(details.isAlive()))

        # create msgService to send stop message.
        myThread = threading.currentThread()
        factory = WMFactory("msgService", "WMCore.MsgService." + \
                            myThread.dialect)
        myThread.transaction = Transaction(myThread.dbi)
        msgService = factory.loadObject("MsgService")
        msgService.registerAs("HarnessTest")
        myThread.transaction.commit()

        print('Publish a stop message to test if the component shutsdown gracefully')
        myThread.transaction.begin()
        msg = {'name': 'Stop', 'payload': ''}
        msgService.publish(msg)
        myThread.transaction.commit()

        msgService.finish()

        while details.isAlive():
            print('Component has not received stop message')
            time.sleep(2)
        print('Daemon shutdown gracefully')


if __name__ == '__main__':
    unittest.main()
