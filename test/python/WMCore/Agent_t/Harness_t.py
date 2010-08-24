#!/usr/bin/env python
"""
Component test TestComponent module and the harness
"""

__revision__ = "$Id: Harness_t.py,v 1.1 2008/08/26 13:55:16 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "fvlingen@caltech.edu"

import logging
import os
import random
import threading
import unittest


from WMCore.Database.DBFactory import DBFactory
from WMCore.WMFactory import WMFactory


from TestComponent import TestComponent

class HarnessTest(unittest.TestCase):
    """
    TestCase for TestComponent module 
    """

    _setup_done = False
    _log_level = 'debug'

    def setUp(self):
        """
        setup for test.
        """
        if not HarnessTest._setup_done:
            logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                datefmt='%m-%d %H:%M',
                filename='%s.log' % __file__,
                filemode='w')

            myThread = threading.currentThread()
            myThread.logger = logging.getLogger('MsgServiceTest')
            myThread.dialect = 'MySQL'

            options = {}
            options['unix_socket'] = os.getenv("DBSOCK")
            dbFactory = DBFactory(myThread.logger, os.getenv("MYSQLDATABASE"), \
                options)

            myThread.dbi = dbFactory.connect()

            factory = WMFactory("msgService", "WMCore.MsgService."+ \
                myThread.dialect)
            create = factory.loadObject("Create")
            createworked = create.execute()
            if createworked:
                logging.debug("MsgService tables created")
            else:
                logging.debug("MsgService tables could not be created, \
                    already exists?")

            HarnessTest._setup_done = True

    def testA(self):
        # parameters for test component:
        args = {}
        args['workDir'] = os.getenv("TESTDIR")
        args['db_dialect'] = 'mysql'
        args['db_socket'] = os.getenv("DBSOCK")
        args['db_user'] = os.getenv("DBUSER")
        args['db_pass'] = os.getenv("DBPASS")
        args['db_hostname'] = os.getenv("DBHOST")
        args['db_name'] = os.getenv("DBNAME")

        testComponent = TestComponent(**args)
        testComponent.prepareToStart()
        testComponent.handleMessage('LogState','')
        testComponent.handleMessage('TestMessage1','TestMessag1Payload')
        testComponent.handleMessage('TestMessage2','TestMessag2Payload')
        testComponent.handleMessage('TestMessage3','TestMessag3Payload')
        testComponent.handleMessage('TestMessage4','TestMessag4Payload')

if __name__ == '__main__':
    unittest.main()

