#!/usr/bin/env python
#pylint: disable-msg=E1101,C0103,R0902
"""
DBSBuffer test TestDBSBuffer module and the harness
"""

__revision__ = "$Id: DBSBuffer_t.py,v 1.2 2008/10/03 12:36:04 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "fvlingen@caltech.edu"

import commands
import logging
import os
import threading
import time
import unittest

from WMComponent.DBSBuffer.DBSBuffer import DBSBuffer

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.Transaction import Transaction
from WMCore.WMFactory import WMFactory

class DBSBufferTest(unittest.TestCase):
    """
    TestCase for TestDBSBuffer module 
    """

    _setup_done = False
    _teardown = False
    _maxMessage = 10

    def setUp(self):
        """
        setup for test.
        """
        if not DBSBufferTest._setup_done:
            logging.basicConfig(level=logging.NOTSET,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            myThread = threading.currentThread()
            myThread.logger = logging.getLogger('DBSBufferTest')
            myThread.dialect = 'MySQL'

            options = {}
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("DATABASE"), \
                options)

            myThread.dbi = dbFactory.connect()
            myThread.transaction = Transaction(myThread.dbi)

            for factoryName in ["WMCore.MsgService", "WMCore.ThreadPool", \
            "WMComponent.DBSBuffer.Database"]:
                # need to create these tables for testing.
                factory = WMFactory(factoryName, factoryName + "." + \
                    myThread.dialect)
                create = factory.loadObject("Create")
                createworked = create.execute(conn = myThread.transaction.conn)
                if createworked:
                    logging.debug("Tables for "+ factoryName + " created")
                else:
                    logging.debug("Tables " + factoryName + \
                    " could not be created, already exists?")
            myThread.transaction.commit()

            DBSBufferTest._setup_done = True

    def tearDown(self):
        """
        Database deletion
        """
        myThread = threading.currentThread()
        if DBSBufferTest._teardown and myThread.dialect == 'MySQL':
            # call the script we use for cleaning:
            command = os.getenv('WMCOREBASE')+ '/standards/./cleanup_mysql.sh'
            result = commands.getstatusoutput(command)
            for entry in result:
                print(str(entry))

        DBSBufferTest._teardown = False


    def testA(self):
        """
        Mimics creation of component and handles come messages.
        """
        # read the default config first.
        config = loadConfigurationFile(os.path.join(os.getenv('WMCOREBASE'), \
            'src/python/WMComponent/DBSBuffer/DefaultConfig.py'))

        # we set the maxRetries to 10 for testing purposes
        config.DBSBuffer.maxRetries = 10
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
        config.CoreDatabase.connectUrl = os.getenv("DATABASE")

        testDBSBuffer = DBSBuffer(config)
        testDBSBuffer.prepareToStart()
        # for testing purposes we use this method instead of the 
        # StartComponent one.
        testDBSBuffer.handleMessage('LogState','')
        for i in xrange(0, DBSBufferTest._maxMessage):
            testDBSBuffer.handleMessage('JobSuccess', \
                'YourMessageHere'+str(i))

        # wait until all threads finish to check list size 
        while threading.activeCount() > 1:
            print('Currently: '+str(threading.activeCount())+\
                ' Threads. Wait until all our threads have finished')
            time.sleep(1)

        DBSBufferTest._teardown = True

    def runTest(self):
        """
        Run error handler test.
        """
        self.testA()

if __name__ == '__main__':
    unittest.main()

