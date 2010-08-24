#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
ErrorHandler test TestErrorHandler module and the harness
"""

__revision__ = "$Id: ErrorHandler_t.py,v 1.3 2008/09/26 14:48:05 fvlingen Exp $"
__version__ = "$Revision: 1.3 $"
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


            # need to create these tables for testing.
            factory = WMFactory("msgService", "WMCore.MsgService."+ \
                myThread.dialect)
            create = factory.loadObject("Create")
            createworked = create.execute(conn = myThread.transaction.conn)
            if createworked:
                logging.debug("MsgService tables created")
            else:
                logging.debug("MsgService tables could not be created, \
                    already exists?")
            # as the example uses threads we need to create the thread
            # tables too.
            factory = WMFactory("msgService", "WMCore.ThreadPool."+ \
                myThread.dialect)
            create = factory.loadObject("Create")
            createworked = create.execute(conn = myThread.transaction.conn)
            if createworked:
                logging.debug("ThreadPool tables created")
            else:
                logging.debug("ThreadPool tables could not be created, \
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

        testErrorHandler = ErrorHandler(config)
        testErrorHandler.prepareToStart()
        # for testing purposes we use this method instead of the 
        # StartComponent one.
        testErrorHandler.handleMessage('LogState','')
        for i in xrange(0, ErrorHandlerTest._maxMessage):
            testErrorHandler.handleMessage('SubmitFailure', \
                'SubmitFailurePayload'+str(i))
            testErrorHandler.handleMessage('CreateFailure', \
                'CreateFailurePayload'+str(i))
            testErrorHandler.handleMessage('RunFailure', \
                'RunFailurePayload'+str(i))
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')

        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)
        ErrorHandlerTest._teardown = True

    def runTest(self):
        self.testA()
if __name__ == '__main__':
    unittest.main()

