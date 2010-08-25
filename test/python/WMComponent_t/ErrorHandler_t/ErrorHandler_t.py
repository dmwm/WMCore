#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
ErrorHandler test TestErrorHandler module and the harness
"""

__revision__ = "$Id: ErrorHandler_t.py,v 1.8 2009/05/08 13:21:46 afaq Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = "fvlingen@caltech.edu"

import commands
import logging
import os
import threading
import time
import unittest

from WMComponent.ErrorHandler.ErrorHandler import ErrorHandler

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

from WMQuality.TestInit import TestInit

class ErrorHandlerTest(unittest.TestCase):
    """
    TestCase for TestErrorHandler module 
    """

    _setup_done = False
    _teardown = False
    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """
        if not ErrorHandlerTest._setup_done:
            self.testInit = TestInit(__file__)
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            self.testInit.setSchema(["WMComponent.ErrorHandler.Database"])
            ErrorHandlerTest._setup_done = True

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()
        if ErrorHandlerTest._teardown and myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            self.testInit.clearDatabase()
        ErrorHandlerTest._teardown = False

    def testA(self):
        """
        Mimics creation of component and handles come messages.
        """
        ErrorHandlerTest._teardown = True
        # read the default config first.
        config = self.testInit.getConfiguration(\
            os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/ErrorHandler/DefaultConfig.py'))

        # we set the maxRetries to 10 for testing purposes
        config.ErrorHandler.maxRetries = 10

        # load a message service as we want to check if total failure
        # messages are returned
        myThread = threading.currentThread()
        myThread.transaction.begin()
        factory = WMFactory('msgService', 'WMCore.MsgService.'+myThread.dialect)
        msgService =  factory.loadObject("MsgService")
        msgService.registerAs('ErrorHandler_t')
        # subscribe to what we want to test.
        msgService.subscribeTo('FinalJobFailure')
        myThread.transaction.commit()

        testErrorHandler = ErrorHandler(config)
        testErrorHandler.prepareToStart()
        # for testing purposes we use this method instead of the 
        # StartComponent one.
        testErrorHandler.handleMessage('LogState','')
        for i in xrange(0, ErrorHandlerTest._maxMessage):
            for j in xrange(0, 3):
		# PollFailure Message is sent so that the Component start looking for error condition
		# each type of error condition has a seprate handler, so we can just Poll For that consition separately
    		# messages do not take any argument in this case.
                testErrorHandler.handleMessage('PollCreateFailure', '') 
                testErrorHandler.handleMessage('PollSubmitFailure', '') 
                testErrorHandler.handleMessage('PollJobFailure', '') 
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')

        # wait until all threads finish to check list size 
        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        myThread.transaction.begin()
        retrySize = errQueries.count()
        myThread.transaction.commit()


    def runTest(self):
        """
        Run error handler test.
        """
        self.testA()

if __name__ == '__main__':
    unittest.main()

