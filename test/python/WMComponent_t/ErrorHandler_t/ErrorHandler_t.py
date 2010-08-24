#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
ErrorHandler test TestErrorHandler module and the harness
"""

__revision__ = "$Id: ErrorHandler_t.py,v 1.4 2008/09/30 18:25:39 fvlingen Exp $"
__version__ = "$Revision: 1.4 $"
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
            logging.basicConfig(level=logging.NOTSET,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            myThread = threading.currentThread()
            myThread.logger = logging.getLogger('ErrorHandlerTest')
            myThread.dialect = 'MySQL'

            options = {}
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)

            myThread.dbi = dbFactory.connect()
            myThread.transaction = Transaction(myThread.dbi)

            for factoryName in ["WMCore.MsgService", "WMCore.ThreadPool","WMComponent.ErrorHandler.Database"]:
                # need to create these tables for testing.
                factory = WMFactory(factoryName, factoryName + "." + \
                    myThread.dialect)
                create = factory.loadObject("Create")
                createworked = create.execute(conn = myThread.transaction.conn)
                if createworked:
                    logging.debug("Tables for "+ factoryName + " created")
                else:
                    logging.debug("Tables " + factoryName + " could not be created, \
                        already exists?")
            myThread.transaction.commit()

            ErrorHandlerTest._setup_done = True

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()
        if ErrorHandlerTest._teardown and myThread.dialect == 'MySQL':
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
        ErrorHandlerTest._teardown = False


    def testA(self):
        """
        Mimics creation of component and handles come messages.
        """
        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/ErrorHandler/DefaultConfig.py'))

        # we set the maxRetries to 10 for testing purposes
        config.ErrorHandler.maxRetries = 10
        # some general settings that would come from the general default 
        # config file
        config.Agent.contact = "fvlingen@caltech.edu"
        config.Agent.teamName = "Lakers"
        config.Agent.agentName = "Lebron James"

        config.section_("General")
        config.General.workDir = os.getenv("TESTDIR")

       
        config.section_("CoreDatabase")
        config.CoreDatabase.dialect = 'mysql' 
        config.CoreDatabase.socket = os.getenv("DBSOCK")
        config.CoreDatabase.user = os.getenv("DBUSER")
        config.CoreDatabase.passwd = os.getenv("DBPASS")
        config.CoreDatabase.hostname = os.getenv("DBHOST")
        config.CoreDatabase.name = os.getenv("DBNAME")

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

        # load a query interface for errorhandler to count the number
        # of entries in its table.
        errQueries = myThread.factory['WMComponent.ErrorHandler.Database'].loadObject('Queries')

        testErrorHandler = ErrorHandler(config)
        testErrorHandler.prepareToStart()
        # for testing purposes we use this method instead of the 
        # StartComponent one.
        testErrorHandler.handleMessage('LogState','')
        for i in xrange(0, ErrorHandlerTest._maxMessage):
            for j in xrange(0,3):
                testErrorHandler.handleMessage('SubmitFailure', \
                    'JobID'+str(i))
                testErrorHandler.handleMessage('CreateFailure', \
                    'JobID'+str(i))
                testErrorHandler.handleMessage('RunFailure', \
                    'JobID'+str(i))
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

        # there should be 10 entries each have the retries set to 8
        assert retrySize == ErrorHandlerTest._maxMessage

        print('Sending the final failure event')
        # now insert the last failure for most jobs which will trigger
        # the total failure event.
        for i in xrange(0, ErrorHandlerTest._maxMessage-5):
            testErrorHandler.handleMessage('SubmitFailure', \
                'JobID'+str(i))
            testErrorHandler.handleMessage('SubmitFailure', \
                'JobID'+str(i))

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        myThread.transaction.begin()
        retrySize = errQueries.count()
        myThread.transaction.commit()
        assert retrySize == 5

        print('Verifying sending of FinalJobFailure messages')
        # failures have been published. retrieve them
        for i in xrange(0, ErrorHandlerTest._maxMessage-5):
            msg = msgService.get()
            assert msg['name'] == 'FinalJobFailure'
            msgService.finish()
        ErrorHandlerTest._teardown = True

        # also send some job success 
        print('Sending some job success messages')
        for i in xrange(ErrorHandlerTest._maxMessage-5, ErrorHandlerTest._maxMessage):
            testErrorHandler.handleMessage('JobSuccess', \
                'JobID'+str(i))

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        myThread.transaction.begin()
        retrySize = errQueries.count()
        myThread.transaction.commit()
        assert retrySize == 0

    def runTest(self):
        self.testA()
if __name__ == '__main__':
    unittest.main()

